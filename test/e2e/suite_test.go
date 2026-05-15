//go:build e2e

/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package e2e_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnpgclaimv1alpha1 "github.com/wyvernzora/cnpg-dbclaim-operator/api/v1alpha1"
)

const (
	cnpgNamespace     = "cnpg-system"
	clusterName       = "shared-pg"
	operatorNamespace = "cnpg-dbclaim-system"
	operatorName      = "dbclaim-dbclaim-operator"
)

var (
	k8sClient  client.Client
	e2eScheme  *runtime.Scheme
	kubeconfig string
	pgPort     int
	stopPG     func()
)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E Suite")
}

var _ = BeforeSuite(func() {
	ctx := context.Background()
	kubeconfig = os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		home, err := os.UserHomeDir()
		Expect(err).NotTo(HaveOccurred())
		kubeconfig = filepath.Join(home, ".kube", "config")
	}

	restCfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	Expect(err).NotTo(HaveOccurred())

	e2eScheme = runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(e2eScheme))
	utilruntime.Must(cnpgv1.AddToScheme(e2eScheme))
	utilruntime.Must(cnpgclaimv1alpha1.AddToScheme(e2eScheme))

	k8sClient, err = client.New(restCfg, client.Options{Scheme: e2eScheme})
	Expect(err).NotTo(HaveOccurred())

	Eventually(func(g Gomega) {
		var cluster cnpgv1.Cluster
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: clusterName, Namespace: cnpgNamespace}, &cluster)).To(Succeed())
		cond := meta.FindStatusCondition(cluster.Status.Conditions, string(cnpgv1.ConditionClusterReady))
		g.Expect(cond).NotTo(BeNil())
		g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
	}, 2*time.Minute, 2*time.Second).Should(Succeed())

	Eventually(func(g Gomega) {
		var deploy appsv1.Deployment
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: operatorName, Namespace: operatorNamespace}, &deploy)).To(Succeed())
		g.Expect(deploy.Status.AvailableReplicas).To(BeNumerically(">=", int32(1)))
	}, 2*time.Minute, 2*time.Second).Should(Succeed())

	pgPort, stopPG, err = portForwardPostgres(ctx, kubeconfig, cnpgNamespace, clusterName+"-rw")
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	if stopPG != nil {
		stopPG()
	}
})

var _ = AfterEach(func() {
	if CurrentSpecReport().Failed() {
		dumpState(context.Background())
	}
})

func superuserSecret(ctx context.Context) *corev1.Secret {
	var secret corev1.Secret
	Expect(k8sClient.Get(ctx, client.ObjectKey{Name: clusterName + "-superuser", Namespace: cnpgNamespace}, &secret)).To(Succeed())
	return &secret
}
