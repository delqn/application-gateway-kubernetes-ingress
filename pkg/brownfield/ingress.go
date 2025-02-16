// -------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
// --------------------------------------------------------------------------------------------

package brownfield

import (
	"k8s.io/api/extensions/v1beta1"

	ptv1 "github.com/Azure/application-gateway-kubernetes-ingress/pkg/apis/azureingressprohibitedtarget/v1"
)

// PruneIngressRules transforms the given ingress struct to remove targets, which AGIC should not create configuration for.
func PruneIngressRules(ing *v1beta1.Ingress, prohibitedTargets []*ptv1.AzureIngressProhibitedTarget) []v1beta1.IngressRule {

	if ing.Spec.Rules == nil || len(ing.Spec.Rules) == 0 {
		return ing.Spec.Rules
	}

	blacklist := GetTargetBlacklist(prohibitedTargets)

	if blacklist == nil || len(*blacklist) == 0 {
		return ing.Spec.Rules
	}

	var rules []v1beta1.IngressRule

	for _, rule := range ing.Spec.Rules {
		if rule.HTTP == nil {
			continue
		}
		target := Target{
			Hostname: rule.Host,
		}
		if rule.HTTP.Paths == nil {
			if target.IsBlacklisted(blacklist) {
				continue
			}
			rules = append(rules, rule)
			continue // to next rule
		}

		newRule := v1beta1.IngressRule{
			Host: rule.Host,
			IngressRuleValue: v1beta1.IngressRuleValue{
				HTTP: &v1beta1.HTTPIngressRuleValue{
					Paths: []v1beta1.HTTPIngressPath{},
				},
			},
		}
		for _, path := range rule.HTTP.Paths {
			target.Path = path.Path
			if target.IsBlacklisted(blacklist) {
				continue
			}
			newRule.HTTP.Paths = append(newRule.HTTP.Paths, path)
		}
		if len(newRule.HTTP.Paths) > 0 {
			rules = append(rules, newRule)
		}
	}

	return rules
}
