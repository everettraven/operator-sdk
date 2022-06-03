// Copyright 2022 The Operator-SDK Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package fbcindex

import (
	"encoding/json"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	declarativeconfig "github.com/operator-framework/operator-registry/alpha/declcfg"
	"github.com/operator-framework/operator-registry/alpha/property"
	"github.com/operator-framework/operator-sdk/internal/olm/fbcutil"
)

func TestFBCRegistryPod(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Test FBC Registry Pod Suite")
}

var _ = Describe("FBC Registry Pod tests", func() {
	Context("partition helper functions", func() {
		var (
			partition1        *declarativeconfig.DeclarativeConfig
			partition1Package declarativeconfig.Package
			partition2        *declarativeconfig.DeclarativeConfig
			partition2Package declarativeconfig.Package

			fbc *declarativeconfig.DeclarativeConfig

			fbcContents        string
			partition1Contents string
			partition2Contents string
		)
		BeforeEach(func() {
			partition1Package = declarativeconfig.Package{
				Schema:         "olm.package",
				Name:           "test-package-1",
				Description:    "test-package-1 description",
				DefaultChannel: "alpha",
			}

			partition1 = &declarativeconfig.DeclarativeConfig{
				Packages: []declarativeconfig.Package{
					partition1Package,
				},
				Channels: []declarativeconfig.Channel{
					{
						Schema:  "olm.channel",
						Name:    "alpha",
						Package: "test-package-1",
						Entries: []declarativeconfig.ChannelEntry{
							{
								Name:     "test-package-1.v0.0.2",
								Replaces: "test-package-1.v0.0.1",
							},
							{
								Name: "test-package-1.v0.0.1",
							},
						},
					},
				},
				Bundles: []declarativeconfig.Bundle{
					{
						Schema:  "olm.bundle",
						Name:    "test-package-1.v0.0.1",
						Package: "test-package-1",
						Image:   "quay.io/test/test-package-1:v0.0.1",
						Properties: []property.Property{
							{
								Type:  "olm.package",
								Value: json.RawMessage(`{"packageName":"test-package-1", "version":"0.0.1"}`),
							},
						},
					},
					{
						Schema:  "olm.bundle",
						Name:    "test-package-1.v0.0.2",
						Package: "test-package-1",
						Image:   "quay.io/test/test-package-1:v0.0.2",
						Properties: []property.Property{
							{
								Type:  "olm.package",
								Value: json.RawMessage(`{"packageName":"test-package-1", "version":"0.0.2"}`),
							},
						},
					},
				},
			}

			partition2Package = declarativeconfig.Package{
				Schema:         "olm.package",
				Name:           "test-package-2",
				Description:    "test-package-2 description",
				DefaultChannel: "alpha",
			}

			partition2 = &declarativeconfig.DeclarativeConfig{
				Packages: []declarativeconfig.Package{
					partition2Package,
				},
				Channels: []declarativeconfig.Channel{
					{
						Schema:  "olm.channel",
						Name:    "alpha",
						Package: "test-package-2",
						Entries: []declarativeconfig.ChannelEntry{
							{
								Name: "test-package-2.v0.0.1",
							},
						},
					},
				},
				Bundles: []declarativeconfig.Bundle{
					{
						Schema:  "olm.bundle",
						Name:    "test-package-2.v0.0.1",
						Package: "test-package-2",
						Image:   "quay.io/test/test-package-2:v0.0.1",
						Properties: []property.Property{
							{
								Type:  "olm.package",
								Value: json.RawMessage(`{"packageName":"test-package-2", "version":"0.0.1"}`),
							},
						},
					},
				},
			}

			packages := []declarativeconfig.Package{}
			packages = append(packages, partition1.Packages...)
			packages = append(packages, partition2.Packages...)

			channels := []declarativeconfig.Channel{}
			channels = append(channels, partition1.Channels...)
			channels = append(channels, partition2.Channels...)

			bundles := []declarativeconfig.Bundle{}
			bundles = append(bundles, partition1.Bundles...)
			bundles = append(bundles, partition2.Bundles...)

			others := []declarativeconfig.Meta{}
			others = append(others, partition1.Others...)

			fbc = &declarativeconfig.DeclarativeConfig{
				Packages: packages,
				Channels: channels,
				Bundles:  bundles,
				Others:   others,
			}

			var err error
			fbcContents, err = fbcutil.ValidateAndStringify(fbc)
			Expect(err).NotTo(HaveOccurred())

			partition1Contents, err = fbcutil.ValidateAndStringify(partition1)
			Expect(err).NotTo(HaveOccurred())

			partition2Contents, err = fbcutil.ValidateAndStringify(partition2)
			Expect(err).NotTo(HaveOccurred())
		})

		It("getDeclarativeConfigForPackage() should return declarative config for test-package-1", func() {
			declcfg := getDeclarativeConfigForPackage(partition1Package, fbc)

			Expect(declcfg.Packages).Should(Equal(partition1.Packages))
			Expect(declcfg.Bundles).Should(Equal(partition1.Bundles))
			Expect(declcfg.Channels).Should(Equal(partition1.Channels))
			Expect(declcfg.Others).Should(Equal(partition1.Others))
		})

		It("getDeclarativeConfigForPackage() should return declarative config for test-package-2", func() {
			declcfg := getDeclarativeConfigForPackage(partition2Package, fbc)

			Expect(declcfg.Packages).Should(Equal(partition2.Packages))
			Expect(declcfg.Bundles).Should(Equal(partition2.Bundles))
			Expect(declcfg.Channels).Should(Equal(partition2.Channels))
			Expect(declcfg.Others).Should(Equal(partition2.Others))
		})

		It("partitionFBC() should return a map of packages to FBC contents", func() {
			partitions, err := partitionFBC(fbcContents)
			Expect(err).NotTo(HaveOccurred())
			Expect(partitions).Should(HaveKeyWithValue("test-package-1", partition1Contents))
			Expect(partitions).Should(HaveKeyWithValue("test-package-2", partition2Contents))
		})

		It("getConfigMaps() should return 2 ConfigMaps with FBC contents", func() {
			configMaps, err := getConfigMaps(fbcContents, "namespace")
			Expect(err).NotTo(HaveOccurred())

			Expect(len(configMaps)).Should(Equal(2))

			Expect(configMaps[0].Kind).Should(Equal("ConfigMap"))
			Expect(configMaps[0].Name).Should(Equal("operator-sdk-run-bundle-config-test-package-1"))
			Expect(configMaps[0].Namespace).Should(Equal("namespace"))
			Expect(configMaps[0].Data).Should(HaveKeyWithValue("test-package-1", partition1Contents))

			Expect(configMaps[1].Kind).Should(Equal("ConfigMap"))
			Expect(configMaps[1].Name).Should(Equal("operator-sdk-run-bundle-config-test-package-2"))
			Expect(configMaps[1].Namespace).Should(Equal("namespace"))
			Expect(configMaps[1].Data).Should(HaveKeyWithValue("test-package-2", partition2Contents))
		})
	})
})
