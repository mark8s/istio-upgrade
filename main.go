package main

import (
	"context"
	"fmt"
	"io"
	"istio.io/pkg/log"
	v12 "k8s.io/api/core/v1"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"
	structpb "google.golang.org/protobuf/types/known/structpb"
	operatorv1alpha1 "istio.io/api/operator/v1alpha1"
	"istio.io/istio/operator/cmd/mesh"
	iopv1alpha1 "istio.io/istio/operator/pkg/apis/istio/v1alpha1"
	"istio.io/istio/operator/pkg/cache"
	"istio.io/istio/operator/pkg/controller/istiocontrolplane"
	"istio.io/istio/operator/pkg/helmreconciler"
	"istio.io/istio/operator/pkg/manifest"
	"istio.io/istio/operator/pkg/name"
	"istio.io/istio/operator/pkg/object"
	"istio.io/istio/operator/pkg/util/clog"
	"istio.io/istio/operator/pkg/util/progress"
	"istio.io/istio/pkg/kube"
	pkgversion "istio.io/pkg/version"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	Version             = ""
	upgradeControlPlane = pflag.BoolP("control-plane", "p", false, "Upgrade Istio control plane")
	upgradeGateway      = pflag.BoolP("gateways", "g", false, "Upgrade Istio gateways")
	upgradeDataPlane    = pflag.BoolP("data-plane", "d", false, "Upgrade Istio data plane")
	namespace           = pflag.StringSlice("namespaces", nil, "Kubernetes namespace")
	names               = pflag.StringSlice("names", nil, "Kubernetes workload names")
	gatewayNames        = pflag.StringSlice("gateway", nil, "Specify gateway name to be upgraded")
	allGateways         = pflag.BoolP("all", "A", false, "Upgrade all gateways")
	iopFile             = pflag.StringP("filename", "f", "", "Path to file containing IstioOperator custom resource. The installed IstioOperator will be used if not specify.")
	helmValueFile       = pflag.StringP("values", "v", "", "Path to file containing helm values")

	clusterContext    = pflag.String("context", "", "The name of the kubeconfig context to use")
	clusterConfigFile = pflag.StringP("kubeconfig", "c", "", "Kubernetes configuration file")
	timeout           = pflag.Duration("readiness-timeout", 300*time.Second, "Maximum time to wait for Istio resources in each component to be ready.")
	dryRun            = pflag.Bool("dry-run", false, "Console/log output only, make no changes.")
	verbose           = pflag.Bool("verbose", false, "Enable verbose log")
	hub               = pflag.String("hub", "docker.io/istio", "Istio hub. default docker.io/istio")

	sample = `
To upgrade both control plane and all gateways,
istio-upgrade -p -g -A

To upgrade only control plane
istio-upgrade -p

To upgrade all gateways
istio-upgrade -g -A

To upgrade given gateways
istio-upgrade -g --gateway ingress-foo --gateway egress-bar

To upgrade all data-plane
istio-upgrade -d 

To upgrade all data-planes under the specified namespace
istio-upgrade -d --namespaces foo,bar

To upgrade all data-planes under the specified namespace,name 
istio-upgrade -d --namespaces foo --names bar
`
)

func init() {
	pflag.StringVarP(&Version, "version", "r", Version, "Target control plane version or revision for the command.")
}

func main() {
	pflag.Parse()
	fmt.Println(sample)

	force := false
	var err error
	var logger clog.Logger
	if *verbose {
		logger = clog.NewDefaultLogger()
	} else {
		logger = clog.NewConsoleLogger(io.Discard, io.Discard, nil)
	}

	if len(Version) == 0 {
		fmt.Println("Specify the target version via -r")
		pflag.Usage()
		return
	}

	if !*upgradeControlPlane && !*upgradeGateway && !*upgradeDataPlane {
		fmt.Println("Specify the upgrade option via -p or -g")
		pflag.Usage()
		return
	}

	targetRevision := strings.Replace(Version, ".", "-", -1)
	flags := []string{
		fmt.Sprintf("revision=%s", targetRevision),
	}

	kubeClient, client, err := mesh.KubernetesClients(*clusterConfigFile, *clusterContext, logger)
	if err != nil {
		panic(err)
	}

	if *upgradeDataPlane {
		UpgradeDataPlane(kubeClient, *namespace, *names, targetRevision)
		return
	}

	currentRevision := kubeClient.Revision()
	pkgversion.DockerInfo.Hub = "docker.io/istio"
	pkgversion.DockerInfo.Tag = Version
	fmt.Println("Current Istio Revision:", currentRevision, "Hub:", pkgversion.DockerInfo.Hub, "Tag:", pkgversion.DockerInfo.Tag)

	// 1. Load or generate IstioOperator from the stored or given manifests or Helm chart values.
	var iop *iopv1alpha1.IstioOperator
	if len(*helmValueFile) > 0 {
		values, err := os.ReadFile(strings.TrimSpace(*helmValueFile))
		if err != nil {
			panic(err)
		}

		valuesMap := make(map[string]interface{})
		if err := yaml.Unmarshal(values, valuesMap); err != nil {
			panic(err)
		}

		iopValues, err := structpb.NewStruct(valuesMap)
		if err != nil {
			panic(err)
		}

		iop = &iopv1alpha1.IstioOperator{
			TypeMeta: v1.TypeMeta{
				Kind:       "IstioOperator",
				APIVersion: "install.istio.io/v1alpha1",
			},
			Spec: &operatorv1alpha1.IstioOperatorSpec{
				Values: iopValues,
			},
		}

		*iopFile = writeIop(iop)
	} else {
		if len(*iopFile) == 0 {
			*iopFile = fetchIop(client, currentRevision, targetRevision)
		} else {
			fmt.Println(`Make sure the "spec.tag" is not set.`)
		}
	}

	var iopStr string
	iopStr, iop, err = manifest.GenerateConfig([]string{*iopFile}, flags, force, kubeClient, logger)
	if err != nil {
		panic(err)
	}

	// 2. Replicate the IstioOperator for Helm rendering. Keep the original IstioOperator isn't changed.
	helmIop := &iopv1alpha1.IstioOperator{}
	if err := yaml.Unmarshal([]byte(iopStr), helmIop); err != nil {
		panic(err)
	}

	// 2.1 If only gateways are upgraded, clear charts core components and dependencies of charts of gateways.
	if !*upgradeControlPlane {
		name.AllCoreComponentNames = nil
		helmreconciler.ComponentDependencies = make(map[name.ComponentName][]name.ComponentName)
	}

	// 2.2 If control plane is upgraded, remove the gateway configuration from IstioOperator for Helm.
	if !*upgradeGateway {
		helmIop.Spec.Components.EgressGateways = nil
		helmIop.Spec.Components.IngressGateways = nil
	}

	helmIop.Spec.Hub = *hub

	// 2.3 Keep only given gateways in IstioOperator for Helm.
	if !*allGateways {
		var egress, ingress []*operatorv1alpha1.GatewaySpec
		if len(*gatewayNames) > 0 {
			selectedGateways := make(map[string]bool, len(*gatewayNames))
			for _, n := range *gatewayNames {
				selectedGateways[n] = true
			}

			for _, g := range helmIop.Spec.Components.EgressGateways {
				if selectedGateways[g.Name] {
					egress = append(egress, g)
				}
			}

			for _, g := range helmIop.Spec.Components.IngressGateways {
				if selectedGateways[g.Name] {
					ingress = append(ingress, g)
				}
			}

			if len(egress) == 0 && len(ingress) == 0 {
				panic("no gateway is selected")
			}

			helmIop.Spec.Components.EgressGateways = egress
			helmIop.Spec.Components.IngressGateways = ingress
		}
	}

	// 3. Render Helm charts and apply them.
	reconciler, err := installManifests(helmIop, force, *dryRun, kubeClient, client, *timeout, logger)
	if err != nil {
		panic(err)
	}

	// 4. Save the original one rather than the IstioOperator for Helm.
	if err = saveIop(iop, reconciler); err != nil {
		panic(err)
	}
}

func installManifests(iop *iopv1alpha1.IstioOperator, force bool, dryRun bool, kubeClient kube.Client, client client.Client,
	waitTimeout time.Duration, l clog.Logger,
) (*helmreconciler.HelmReconciler, error) {
	cache.FlushObjectCaches()
	opts := &helmreconciler.Options{
		DryRun: dryRun, Log: l, WaitTimeout: waitTimeout, ProgressLog: progress.NewLog(),
		Force: force,
	}
	reconciler, err := helmreconciler.NewHelmReconciler(client, kubeClient, iop, opts)
	if err != nil {
		return nil, err
	}
	status, err := reconciler.Reconcile()
	if err != nil {
		return reconciler, fmt.Errorf("errors occurred during operation: %v", err)
	}
	if status.Status != operatorv1alpha1.InstallStatus_HEALTHY {
		return reconciler, fmt.Errorf("errors occurred during operation")
	}

	opts.ProgressLog.SetState(progress.StateComplete)
	return reconciler, nil
}

func saveIop(iop *iopv1alpha1.IstioOperator, reconciler *helmreconciler.HelmReconciler) error {
	// Save a copy of what was installed as a CR in the cluster under an internal name.
	iop.Name = savedIOPName(iop)
	if iop.Annotations == nil {
		iop.Annotations = make(map[string]string)
	}
	iop.Annotations[istiocontrolplane.IgnoreReconcileAnnotation] = "true"
	iopStr, err := yaml.Marshal(iop)
	if err != nil {
		return err
	}

	return saveIOPToCluster(reconciler, string(iopStr))
}

func savedIOPName(iop *iopv1alpha1.IstioOperator) string {
	ret := name.InstalledSpecCRPrefix
	if iop.Name != "" {
		ret += "-" + iop.Name
	}
	if iop.Spec.Revision != "" {
		ret += "-" + iop.Spec.Revision
	}
	return ret
}

func saveIOPToCluster(reconciler *helmreconciler.HelmReconciler, iop string) error {
	obj, err := object.ParseYAMLToK8sObject([]byte(iop))
	if err != nil {
		return err
	}

	return reconciler.ApplyObject(obj.UnstructuredObject(), false)
}

func fetchIop(client client.Client, currentRevision, targetRevision string) string {
	iopList := iopv1alpha1.IstioOperatorList{}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	err := client.List(ctx, &iopList)
	if err != nil {
		panic(err)
	}

	if len(iopList.Items) == 0 {
		panic("No IstioOperators found")
	}

	var targetIop *iopv1alpha1.IstioOperator
	for i := range iopList.Items {
		item := &iopList.Items[i]
		if *upgradeControlPlane {
			if item.Spec.Revision == currentRevision {
				targetIop = item
				break
			}
		} else if *upgradeGateway {
			if item.Spec.Revision == targetRevision {
				targetIop = item
				break
			}
		}
	}

	if targetIop == nil {
		panic("IstioOperator not found")
	}

	fmt.Println("IstioOperator", targetIop.Name, "/", targetIop.Namespace, " is used")

	iop := &iopv1alpha1.IstioOperator{
		TypeMeta: v1.TypeMeta{
			Kind:       "IstioOperator",
			APIVersion: "install.istio.io/v1alpha1",
		},
		Spec: targetIop.Spec,
	}

	iop.Spec.Tag = nil
	return writeIop(iop)
}

func writeIop(iop *iopv1alpha1.IstioOperator) string {
	bytes, err := yaml.Marshal(iop)
	if err != nil {
		panic(err)
	}

	f, err := os.CreateTemp("", "istio-operator")
	if err != nil {
		panic(err)
	}

	defer f.Close()

	n, err := f.Write(bytes)
	if err != nil || n != len(bytes) {
		panic(fmt.Sprintf("%d: %s", n, err))
	}

	fmt.Println("The IstioOperator is temporarily saved in", f.Name())
	return f.Name()
}

// UpgradeDataPlane Upgrade the data plane for the specified namespace or the specified service name, by setting labels istio.io/rev=revision.
// This will exclude namespaces or services that have not previously been set up for automatic injection.
func UpgradeDataPlane(client kube.CLIClient, namespaces []string, names []string, revision string) {
	log.Info("Ready to upgrade Data-Plane.")
	// namespace 为空
	if len(namespaces) == 0 {
		namespaceList, err := client.Kube().CoreV1().Namespaces().List(context.TODO(), v1.ListOptions{})
		if err != nil {
			panic(err)
		}
		for _, ns := range namespaceList.Items {
			// Exclude namespaces that are not automatically injected labels
			if isNamespaceInjected(ns.Labels) {
				UpdateNamespaceLevelDataPlane(client, &ns, revision)
			}
		}
	} else {
		for _, namespace := range namespaces {
			ns, err := client.Kube().CoreV1().Namespaces().Get(context.TODO(), namespace, v1.GetOptions{})
			if err != nil {
				log.Error(err)
				return
			}

			if !isNamespaceInjected(ns.Labels) {
				continue
			}

			if len(names) == 0 {
				UpdateNamespaceLevelDataPlane(client, ns, revision)
			} else {
				for _, name := range names {
					UpdateWorkloadLevelDataPlane(client, namespace, name, revision)
				}
			}
		}
	}
}

func isWorkloadCannotInject(labels map[string]string) bool {
	if value, ok := labels["sidecar.istio.io/inject"]; ok && value == "false" {
		return true
	}
	return false
}

func isNamespaceInjected(labels map[string]string) bool {
	if value, ok := labels["istio-injection"]; ok && value == "enabled" {
		return true
	}
	if _, ok := labels["istio.io/rev"]; ok {
		return true
	}
	return false
}

// UpdateNamespaceLevelDataPlane Upgrade the data plane of the specified namespace, by setting labels istio.io/rev=revision
func UpdateNamespaceLevelDataPlane(client kube.CLIClient, ns *v12.Namespace, revision string) {
	log.Infof("Ready to upgrade namespace: %s Data-Plane.", ns.Name)

	delete(ns.Labels, "istio-injection")
	ns.Labels["istio.io/rev"] = revision

	_, err := client.Kube().CoreV1().Namespaces().Update(context.TODO(), ns, v1.UpdateOptions{})
	log.Infof("Update namespace: %s", ns.Name)
	if err != nil {
		log.Error(err)
		return
	}

	deploymentList, err := client.Kube().AppsV1().Deployments(ns.Name).List(context.TODO(), v1.ListOptions{})
	if err != nil {
		log.Error(err)
		return
	}
	for _, deploy := range deploymentList.Items {
		if isWorkloadCannotInject(deploy.Spec.Template.Labels) {
			continue
		}

		annotations := deploy.Spec.Template.Annotations
		if annotations == nil {
			annotations = make(map[string]string)
		}
		annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().Format(time.RFC3339)
		deploy.Spec.Template.Annotations = annotations
		_, err := client.Kube().AppsV1().Deployments(ns.Name).Update(context.TODO(), &deploy, v1.UpdateOptions{})
		if err != nil {
			log.Error(err)
			return
		}
		log.Infof("rolling upgrade deployment: %s ,namespace: %s", deploy.Name, ns.Name)
	}

	statefulSetsList, err := client.Kube().AppsV1().StatefulSets(ns.Name).List(context.TODO(), v1.ListOptions{})
	if err != nil {
		log.Error(err)
		return
	}

	for _, sts := range statefulSetsList.Items {
		if isWorkloadCannotInject(sts.Spec.Template.Labels) {
			continue
		}
		annotations := sts.Spec.Template.Annotations
		if annotations == nil {
			annotations = make(map[string]string)
		}
		annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().Format(time.RFC3339)
		sts.Spec.Template.Annotations = annotations
		_, err := client.Kube().AppsV1().StatefulSets(ns.Name).Update(context.TODO(), &sts, v1.UpdateOptions{})
		if err != nil {
			log.Error(err)
			return
		}
		log.Infof("rolling upgrade statefulSets: %s ,namespace: %s", sts.Name, ns.Name)
	}

	daemonSetsList, err := client.Kube().AppsV1().DaemonSets(ns.Name).List(context.TODO(), v1.ListOptions{})
	if err != nil {
		log.Error(err)
		return
	}

	for _, ds := range daemonSetsList.Items {
		if isWorkloadCannotInject(ds.Spec.Template.Labels) {
			continue
		}
		annotations := ds.Spec.Template.Annotations
		if annotations == nil {
			annotations = make(map[string]string)
		}
		annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().Format(time.RFC3339)
		ds.Spec.Template.Annotations = annotations
		_, err := client.Kube().AppsV1().DaemonSets(ns.Name).Update(context.TODO(), &ds, v1.UpdateOptions{})
		if err != nil {
			log.Error(err)
			return
		}
		log.Infof("rolling upgrade daemonSets: %s ,namespace: %s", ds.Name, ns.Name)
	}
}

func UpdateWorkloadLevelDataPlane(client kube.CLIClient, namespace, name, revision string) {
	log.Infof("Ready to upgrade namespace: %s Data-Plane.", name)
	deploy, err := client.Kube().AppsV1().Deployments(namespace).Get(context.TODO(), name, v1.GetOptions{})
	if err != nil {
		return
	}

	if deploy != nil {
		labels := deploy.Spec.Template.Labels
		if !isWorkloadCannotInject(labels) {
			if labels == nil {
				labels = make(map[string]string)
			} else {
				delete(labels, "istio-injection")
				labels["istio.io/rev"] = revision
			}
			deploy.Spec.Template.Labels = labels
			annotations := deploy.Spec.Template.Annotations
			if annotations == nil {
				annotations = make(map[string]string)
			}
			annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().Format(time.RFC3339)
			deploy.Spec.Template.Annotations = annotations
			_, err = client.Kube().AppsV1().Deployments(namespace).Update(context.TODO(), deploy, v1.UpdateOptions{})
			log.Infof("rolling upgrade Deployment: %s ,namespace: %s", name, namespace)
		}
	}

	sts, err := client.Kube().AppsV1().StatefulSets(namespace).Get(context.TODO(), name, v1.GetOptions{})
	if err != nil {
		return
	}

	if sts != nil {
		labels := sts.Spec.Template.Labels
		if !isWorkloadCannotInject(labels) {
			if labels == nil {
				labels = make(map[string]string)
			} else {
				delete(labels, "istio-injection")
				labels["istio.io/rev"] = revision
			}
			sts.Spec.Template.Labels = labels
			annotations := sts.Spec.Template.Annotations
			if annotations == nil {
				annotations = make(map[string]string)
			}
			annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().Format(time.RFC3339)
			sts.Spec.Template.Annotations = annotations
			_, err = client.Kube().AppsV1().StatefulSets(namespace).Update(context.TODO(), sts, v1.UpdateOptions{})
			log.Infof("rolling upgrade StatefulSet: %s ,namespace: %s", name, namespace)
		}
	}

	ds, err := client.Kube().AppsV1().DaemonSets(namespace).Get(context.TODO(), name, v1.GetOptions{})
	if err != nil {
		return
	}

	if ds != nil {
		labels := ds.Spec.Template.Labels
		if !isWorkloadCannotInject(labels) {
			if labels == nil {
				labels = make(map[string]string)
			} else {
				delete(labels, "istio-injection")
				labels["istio.io/rev"] = revision
			}
			ds.Spec.Template.Labels = labels
			annotations := ds.Spec.Template.Annotations
			if annotations == nil {
				annotations = make(map[string]string)
			}
			annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().Format(time.RFC3339)
			ds.Spec.Template.Annotations = annotations
			_, err = client.Kube().AppsV1().DaemonSets(namespace).Update(context.TODO(), ds, v1.UpdateOptions{})
			log.Infof("rolling upgrade DaemonSet: %s ,namespace: %s", name, namespace)
		}
	}
}
