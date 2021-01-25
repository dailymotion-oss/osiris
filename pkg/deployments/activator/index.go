package activator

import (
	"context"
	"fmt"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strings"

	"github.com/golang/glog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// nolint: lll
var (
	loadBalancerHostnameAnnotationRegex = regexp.MustCompile(`^osiris\.dm\.gg/loadBalancerHostname(?:-\d+)?$`)
	ingressHostnameAnnotationRegex      = regexp.MustCompile(`^osiris\.dm\.gg/ingressHostname(?:-\d+)?$`)
)

// updateIndex builds an index that maps all the possible ways a service can be
// addressed to application info that encapsulates details like which deployment
// or statefulSet to activate and where to relay requests to after successful
// activation. The new index replaces any old/existing index.
func (a *activator) updateIndex() {
	ctx := context.Background()
	appsByHost := map[string]*app{}
	for _, svc := range a.services {
		var (
			name                        string
			kind                        appKind
			dependenciesAnnotationValue string
		)
		if deploymentName, ok :=
			svc.Annotations["osiris.dm.gg/deployment"]; ok {
			name = cleanAnnotationValue(deploymentName)
			kind = appKindDeployment
			deployment, err := a.kubeClient.AppsV1().Deployments(svc.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				glog.Errorf("Error retrieving deployment %s in namespace %s: %s", name, svc.Namespace, err)
				continue
			}
			if deployment.Annotations != nil {
				dependenciesAnnotationValue = cleanAnnotationValue(deployment.Annotations["osiris.dm.gg/dependencies"])
			}
		} else if statefulSetName, ok :=
			svc.Annotations["osiris.dm.gg/statefulset"]; ok {
			name = cleanAnnotationValue(statefulSetName)
			kind = appKindStatefulSet
			statefulset, err := a.kubeClient.AppsV1().StatefulSets(svc.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				glog.Errorf("Error retrieving statefulset %s in namespace %s: %s", name, svc.Namespace, err)
				continue
			}
			if statefulset.Annotations != nil {
				dependenciesAnnotationValue = cleanAnnotationValue(statefulset.Annotations["osiris.dm.gg/dependencies"])
			}
		}
		if len(name) == 0 {
			continue
		}

		// Retrieve the manually-declared dependencies (non-HTTP services)
		dependencies := []*app{}
		for _, dependency := range strings.Split(dependenciesAnnotationValue, ",") {
			if len(dependency) == 0 {
				continue
			}
			elems := strings.SplitN(dependency, ":", 2)
			depKind := elems[0]
			var depAppKind appKind
			switch strings.ToLower(depKind) {
			case "deployment":
				depAppKind = appKindDeployment
			case "statefulset":
				depAppKind = appKindStatefulSet
			default:
				glog.Errorf("Error parsing dependencies annotations for service %s in namespace %s: invalid appKind %s for dependency %s", svc.Name, svc.Namespace, depKind, dependency)
				continue
			}
			elems = strings.SplitN(elems[1], "/", 2)
			depNamespace := elems[0]
			depName := elems[1]
			dependencies = append(dependencies, &app{
				namespace:   depNamespace,
				name:        depName,
				kind:        depAppKind,
				serviceName: depName,
			})
		}

		svcDNSNames := []string{
			fmt.Sprintf("%s", svc.Name),
			fmt.Sprintf("%s.%s", svc.Name, svc.Namespace),
			fmt.Sprintf("%s.%s.svc", svc.Name, svc.Namespace),
			fmt.Sprintf("%s.%s.svc.cluster", svc.Name, svc.Namespace),
			fmt.Sprintf("%s.%s.svc.cluster.local", svc.Name, svc.Namespace),
		}

		// Determine the "default" ingress port. When a request arrives at the
		// activator via an ingress conroller, the request's host header won't
		// indicate a port. After activation is complete, the activator needs to
		// forward the request to the service (which is now backed by application
		// endpoints). It's important to know which service port to forward the
		// request to.
		var ingressDefaultPort string
		var ok bool
		// Start by seeing if a default port was explicitly specified.
		if ingressDefaultPort, ok =
			svc.Annotations["osiris.dm.gg/ingressDefaultPort"]; !ok {
			// If not specified, try to infer it.
			// If there's only one port, that's it.
			if len(svc.Spec.Ports) == 1 {
				ingressDefaultPort = fmt.Sprintf("%d", svc.Spec.Ports[0].Port)
			} else {
				// Look for a port named "http". If found, that's it. While we're
				// looping also look to see if the servie exposes port 80. If no port
				// is named "http", we'll assume 80 (if exposed) is the default port.
				var foundPort80 bool
				for _, port := range svc.Spec.Ports {
					if port.Name == "http" {
						ingressDefaultPort = fmt.Sprintf("%d", port.Port)
						break
					}
					if port.Port == 80 {
						foundPort80 = true
					}
				}
				if ingressDefaultPort == "" && foundPort80 {
					ingressDefaultPort = "80"
				}
			}
		}
		// For every port...
		for _, port := range svc.Spec.Ports {
			targetURL, err :=
				url.Parse(fmt.Sprintf("http://%s:%d", svc.Spec.ClusterIP, port.Port))
			if err != nil {
				glog.Errorf(
					"Error parsing target URL for service %s in namespace %s: %s",
					svc.Name,
					svc.Namespace,
					err,
				)
				continue
			}
			app := &app{
				namespace:           svc.Namespace,
				serviceName:         svc.Name,
				name:                name,
				kind:                kind,
				targetURL:           targetURL,
				proxyRequestHandler: httputil.NewSingleHostReverseProxy(targetURL),
				dependencies:        dependencies,
			}
			// If the port is 80, also index by hostname/IP sans port number...
			if port.Port == 80 {
				// kube-dns names
				for _, svcDNSName := range svcDNSNames {
					appsByHost[svcDNSName] = app
				}
				// cluster IP
				appsByHost[svc.Spec.ClusterIP] = app
				// external IPs
				for _, loadBalancerIngress := range svc.Status.LoadBalancer.Ingress {
					if loadBalancerIngress.IP != "" {
						appsByHost[loadBalancerIngress.IP] = app
					}
				}
				// Honor all annotations of the form
				// ^osiris\.dm\.gg/loadBalancerHostname(?:-\d+)?$
				for k, v := range svc.Annotations {
					if loadBalancerHostnameAnnotationRegex.MatchString(k) {
						hostname := cleanAnnotationValue(v)
						appsByHost[hostname] = app
					}
				}
			}
			if fmt.Sprintf("%d", port.Port) == ingressDefaultPort {
				// Honor all annotations of the form
				// ^osiris\.dm\.gg/ingressHostname(?:-\d+)?$
				for k, v := range svc.Annotations {
					if ingressHostnameAnnotationRegex.MatchString(k) {
						hostname := cleanAnnotationValue(v)
						appsByHost[hostname] = app
					}
				}
			}
			// Now index by hostname/IP:port...
			// kube-dns names
			for _, svcDNSName := range svcDNSNames {
				appsByHost[fmt.Sprintf("%s:%d", svcDNSName, port.Port)] = app
			}
			// cluster IP
			appsByHost[fmt.Sprintf("%s:%d", svc.Spec.ClusterIP, port.Port)] = app
			// external IPs
			for _, loadBalancerIngress := range svc.Status.LoadBalancer.Ingress {
				if loadBalancerIngress.IP != "" {
					appsByHost[fmt.Sprintf("%s:%d", loadBalancerIngress.IP, port.Port)] = app // nolint: lll
				}
			}
			// Node honame/IP:node-port
			if port.NodePort != 0 {
				for nodeAddress := range a.nodeAddresses {
					appsByHost[fmt.Sprintf("%s:%d", nodeAddress, port.NodePort)] = app
				}
			}
			// Honor all annotations of the form
			// ^osiris\.dm\.gg/loadBalancerHostname(?:-\d+)?$
			for k, v := range svc.Annotations {
				if loadBalancerHostnameAnnotationRegex.MatchString(k) {
					hostname := cleanAnnotationValue(v)
					appsByHost[fmt.Sprintf("%s:%d", hostname, port.Port)] = app
				}
			}
		}
	}
	a.appsByHost = appsByHost
}

func cleanAnnotationValue(rawValue string) string {
	value := strings.TrimSpace(rawValue)
	value = strings.TrimLeft(value, "'")
	value = strings.TrimRight(value, "'")
	return value
}
