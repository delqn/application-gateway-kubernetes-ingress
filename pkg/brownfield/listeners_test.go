// -------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
// --------------------------------------------------------------------------------------------

package brownfield

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	ptv1 "github.com/Azure/application-gateway-kubernetes-ingress/pkg/apis/azureingressprohibitedtarget/v1"
	"github.com/Azure/application-gateway-kubernetes-ingress/pkg/tests"
	"github.com/Azure/application-gateway-kubernetes-ingress/pkg/tests/fixtures"
)

var _ = Describe("Test blacklist listeners", func() {

	appGw := fixtures.GetAppGateway()
	defaultListener := (*appGw.HTTPListeners)[0]
	listener1 := (*appGw.HTTPListeners)[1]
	listener2 := (*appGw.HTTPListeners)[2]
	listener3 := (*appGw.HTTPListeners)[3]

	Context("Test GetBlacklistedListeners() with a blacklist", func() {
		It("should create a list of blacklisted and non blacklisted listeners", func() {
			prohibitedTargets := fixtures.GetAzureIngressProhibitedTargets() // Host: "bye.com", Paths: [/fox, /bar]
			er := NewExistingResources(appGw, prohibitedTargets, nil)
			blacklisted, nonBlacklisted := er.GetBlacklistedListeners()

			Expect(len(blacklisted)).To(Equal(3))
			Expect(blacklisted).To(ContainElement(listener1))
			Expect(blacklisted).To(ContainElement(listener2))
			Expect(blacklisted).To(ContainElement(listener3))

			Expect(len(nonBlacklisted)).To(Equal(1))
			Expect(nonBlacklisted).To(ContainElement(defaultListener))
		})
	})

	Context("Test GetBlacklistedListeners() with a blacklist no paths", func() {
		It("should create a list of blacklisted and non blacklisted listeners", func() {
			prohibitedTargets := []*ptv1.AzureIngressProhibitedTarget{
				{
					Spec: ptv1.AzureIngressProhibitedTargetSpec{
						Hostname: tests.Host,
					},
				},
			}
			er := NewExistingResources(appGw, prohibitedTargets, nil)

			blacklisted, nonBlacklisted := er.GetBlacklistedListeners()

			Expect(len(blacklisted)).To(Equal(1))
			Expect(blacklisted).To(ContainElement(listener2))

			Expect(len(nonBlacklisted)).To(Equal(3))
			Expect(nonBlacklisted).To(ContainElement(defaultListener))
			Expect(nonBlacklisted).To(ContainElement(listener1))
			Expect(nonBlacklisted).To(ContainElement(listener3))
		})
	})

	Context("Test GetBlacklistedListeners() with a blacklist with a wildcard", func() {
		It("should create a list of blacklisted and non blacklisted listeners", func() {
			prohibitedTargets := fixtures.GetAzureIngressProhibitedTargets()                    // Host: "bye.com", Paths: [/fox, /bar]
			prohibitedTargets = append(prohibitedTargets, &ptv1.AzureIngressProhibitedTarget{}) // Host: '', Path: []
			er := NewExistingResources(appGw, prohibitedTargets, nil)
			blacklisted, nonBlacklisted := er.GetBlacklistedListeners()

			Expect(len(blacklisted)).To(Equal(4))
			Expect(blacklisted).To(ContainElement(listener1))
			Expect(blacklisted).To(ContainElement(listener2))
			Expect(blacklisted).To(ContainElement(listener3))
			Expect(blacklisted).To(ContainElement(defaultListener))

			Expect(len(nonBlacklisted)).To(Equal(0))
		})
	})

	Context("Test getBlacklistedListenersSet()", func() {
		It("should create a set of blacklisted listeners", func() {
			prohibitedTargets := fixtures.GetAzureIngressProhibitedTargets()
			er := NewExistingResources(appGw, prohibitedTargets, nil)
			set := er.getBlacklistedListenersSet()
			Expect(len(set)).To(Equal(3))
			_, exists := set[fixtures.HTTPListenerPathBased1]
			Expect(exists).To(BeTrue())
			_, exists = set[fixtures.HTTPListenerPathBased2]
			Expect(exists).To(BeTrue())
			_, exists = set[fixtures.HTTPListenerNameBasic]
			Expect(exists).To(BeTrue())
		})
	})

	Context("Test getBlacklistedListenersSet()", func() {
		It("should create a set of blacklisted listeners", func() {
			prohibitedTargets := fixtures.GetAzureIngressProhibitedTargets()
			er := NewExistingResources(appGw, prohibitedTargets, nil)
			er.listenersByName = nil
			listenersByName := er.getListenersByName()
			Expect(er.listenersByName).ToNot(BeNil())

			Expect(len(er.listenersByName)).To(Equal(4))
			Expect(len(listenersByName)).To(Equal(4))

			_, exists := listenersByName[fixtures.HTTPListenerPathBased1]
			Expect(exists).To(BeTrue())
			_, exists = listenersByName[fixtures.HTTPListenerPathBased2]
			Expect(exists).To(BeTrue())
			_, exists = listenersByName[fixtures.HTTPListenerNameBasic]
			Expect(exists).To(BeTrue())
			_, exists = listenersByName[fixtures.DefaultHTTPListenerName]
			Expect(exists).To(BeTrue())
		})
	})

})
