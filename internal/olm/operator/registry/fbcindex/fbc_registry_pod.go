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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	declarativeconfig "github.com/operator-framework/operator-registry/alpha/declcfg"
	"github.com/operator-framework/operator-registry/alpha/property"
	"github.com/operator-framework/operator-sdk/internal/olm/fbcutil"
	"github.com/operator-framework/operator-sdk/internal/olm/operator"
	"github.com/operator-framework/operator-sdk/internal/olm/operator/registry/index"
	"github.com/operator-framework/operator-sdk/internal/util/k8sutil"
)

const (
	// defaultGRPCPort is the default grpc container port that the registry pod exposes
	defaultGRPCPort = 50051

	defaultContainerName     = "registry-grpc"
	defaultContainerPortName = "grpc"

	// DefaultFBCIndexRootDir is the FBC directory that exists under root of an FBC container image.
	// This directory has the File-Based Catalog representation of a catalog index.
	DefaultFBCIndexRootDir = "/configs"
)

// FBCRegistryPod holds resources necessary for creation of a registry pod in FBC scenarios.
type FBCRegistryPod struct { //nolint:maligned
	// BundleItems contains all bundles to be added to a registry pod.
	BundleItems []index.BundleItem

	// Index image contains a database of pointers to operator manifest content that is queriable via an API.
	// new version of an operator bundle when published can be added to an index image
	IndexImage string

	// GRPCPort is the container grpc port
	GRPCPort int32

	// pod represents a kubernetes *corev1.pod that will be created on a cluster using an index image
	pod *corev1.Pod

	// FBCContent represents the contents of the FBC file (string YAML).
	FBCContent string

	// FBCDir is the name of the FBC directory name where the FBC resides in.
	FBCDir string

	// FBCFile represents the FBC filename that has all the contents to be served through the registry pod.
	FBCFile string

	cfg *operator.Configuration
}

// init initializes the FBCRegistryPod struct.
func (f *FBCRegistryPod) init(cfg *operator.Configuration, cs *v1alpha1.CatalogSource) error {
	if f.GRPCPort == 0 {
		f.GRPCPort = defaultGRPCPort
	}

	f.cfg = cfg

	// validate the FBCRegistryPod struct and ensure required fields are set
	if err := f.validate(); err != nil {
		return fmt.Errorf("invalid FBC registry pod: %v", err)
	}

	bundleImage := f.BundleItems[len(f.BundleItems)-1].ImageTag
	trimmedbundleImage := strings.Split(bundleImage, ":")[0]
	f.FBCDir = fmt.Sprintf("%s-index", filepath.Join("/tmp", strings.Split(trimmedbundleImage, "/")[2]))
	f.FBCFile = filepath.Join(f.FBCDir, strings.Split(bundleImage, ":")[1])

	// podForBundleRegistry() to make the pod definition
	pod, err := f.podForBundleRegistry(cs)
	if err != nil {
		return fmt.Errorf("error building registry pod definition: %v", err)
	}
	f.pod = pod

	return nil
}

// Create creates a bundle registry pod built from an fbc index image,
// sets the catalog source as the owner for the pod and verifies that
// the pod is running
func (f *FBCRegistryPod) Create(ctx context.Context, cfg *operator.Configuration, cs *v1alpha1.CatalogSource) (*corev1.Pod, error) {
	if err := f.init(cfg, cs); err != nil {
		return nil, err
	}

	// make catalog source the owner of registry pod object
	if err := controllerutil.SetOwnerReference(cs, f.pod, f.cfg.Scheme); err != nil {
		return nil, fmt.Errorf("error setting owner reference: %w", err)
	}

	if err := f.cfg.Client.Create(ctx, f.pod); err != nil {
		return nil, fmt.Errorf("error creating pod: %w", err)
	}

	// get registry pod key
	podKey := types.NamespacedName{
		Namespace: f.cfg.Namespace,
		Name:      f.pod.GetName(),
	}

	// poll and verify that pod is running
	podCheck := wait.ConditionFunc(func() (done bool, err error) {
		err = f.cfg.Client.Get(ctx, podKey, f.pod)
		if err != nil {
			return false, fmt.Errorf("error getting pod %s: %w", f.pod.Name, err)
		}
		return f.pod.Status.Phase == corev1.PodRunning, nil
	})

	// check pod status to be `Running`
	if err := f.checkPodStatus(ctx, podCheck); err != nil {
		return nil, fmt.Errorf("registry pod did not become ready: %w", err)
	}
	log.Infof("Created registry pod: %s", f.pod.Name)
	return f.pod, nil
}

// checkPodStatus polls and verifies that the pod status is running
func (f *FBCRegistryPod) checkPodStatus(ctx context.Context, podCheck wait.ConditionFunc) error {
	// poll every 200 ms until podCheck is true or context is done
	err := wait.PollImmediateUntil(200*time.Millisecond, podCheck, ctx.Done())
	if err != nil {
		return fmt.Errorf("error waiting for registry pod %s to run: %v", f.pod.Name, err)
	}

	return err
}

// validate will ensure that RegistryPod required fields are set
// and throws error if not set
func (f *FBCRegistryPod) validate() error {
	if len(f.BundleItems) == 0 {
		return errors.New("bundle image set cannot be empty")
	}
	for _, item := range f.BundleItems {
		if item.ImageTag == "" {
			return errors.New("bundle image cannot be empty")
		}
	}

	if f.IndexImage == "" {
		return errors.New("index image cannot be empty")
	}

	return nil
}

func GetRegistryPodHost(ipStr string) string {
	return fmt.Sprintf("%s:%d", ipStr, defaultGRPCPort)
}

// getPodName will return a string constructed from the bundle Image name
func getPodName(bundleImage string) string {
	// todo(rashmigottipati): need to come up with human-readable references
	// to be able to handle SHA references in the bundle images
	return k8sutil.TrimDNS1123Label(k8sutil.FormatOperatorNameDNS1123(bundleImage))
}

// podForBundleRegistry constructs and returns the registry pod definition
// and throws error when unable to build the pod definition successfully
func (f *FBCRegistryPod) podForBundleRegistry(cs *v1alpha1.CatalogSource) (*corev1.Pod, error) {
	// rp was already validated so len(f.BundleItems) must be greater than 0.
	bundleImage := f.BundleItems[len(f.BundleItems)-1].ImageTag

	// construct the container command for pod spec
	containerCmd, err := f.getContainerCmd()
	if err != nil {
		return nil, err
	}

	configMaps, err := getConfigMaps(f.FBCContent, f.cfg.Namespace)
	if err != nil {
		return nil, fmt.Errorf("encountered an error getting ConfigMaps for registry pod: %w", err)
	}

	volumes := []corev1.Volume{}
	volumeMounts := []corev1.VolumeMount{}

	// (todo) remove comment: ConfigMap related
	for _, cm := range configMaps {
		// set owner reference by making catalog source the owner of ConfigMap object
		if err := controllerutil.SetOwnerReference(cs, &cm, f.cfg.Scheme); err != nil {
			return nil, fmt.Errorf("set configmap %q owner reference: %v", cm.GetName(), err)
		}

		// create ConfigMap
		if err := f.cfg.Client.Create(context.TODO(), &cm); err != nil {
			return nil, fmt.Errorf("error creating ConfigMap: %w", err)
		}

		volumes = append(volumes, corev1.Volume{
			Name: k8sutil.TrimDNS1123Label(cm.Name + "-volume"),
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					// Items: []corev1.KeyToPath{
					// 	{
					// 		Key:  cm.Name,
					// 		Path: cm.Name,
					// 	},
					// },
					LocalObjectReference: corev1.LocalObjectReference{
						Name: cm.Name,
					},
				},
			},
		})

		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name: k8sutil.TrimDNS1123Label(cm.Name + "-volume"),
			// MountPath: path.Join(DefaultFBCIndexRootDir, cm.Name),
			MountPath: path.Join(DefaultFBCIndexRootDir),
			// SubPath:   cm.Name,
		})
	}

	// make the pod definition
	f.pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getPodName(bundleImage),
			Namespace: f.cfg.Namespace,
		},
		Spec: corev1.PodSpec{
			Volumes: volumes,
			Containers: []corev1.Container{
				{
					Name:  defaultContainerName,
					Image: f.IndexImage,
					Command: []string{
						"sh",
						"-c",
						containerCmd,
					},
					Ports: []corev1.ContainerPort{
						{Name: defaultContainerPortName, ContainerPort: f.GRPCPort},
					},
					VolumeMounts: volumeMounts,
				},
			},
			// (todo) remove comment (not configmap related).
			// InitContainer related to doing untar of the extra fbc tar.
			// InitContainers: []corev1.Container{
			// 	{
			// 		Name:            "extra-FBC-untar",
			// 		Image:           f.IndexImage, // should this be the same image as regular container?
			// 		ImagePullPolicy: corev1.PullIfNotPresent,
			// 		Args: []string{
			// 			"tar",
			// 			"xvzf",
			// 			"/configs/extrafbc.tar.gz",
			// 			"-C",
			// 			path.Join(defaultFBCIndexRootDir, cm.Name),
			// 		},
			// 		VolumeMounts: []corev1.VolumeMount{
			// 			{
			// 				MountPath: path.Join(defaultFBCIndexRootDir, cm.Name),
			// 				Name:      k8sutil.TrimDNS1123Label(cm.Name + "-volume"),
			// 			},
			// 		},
			// 	},
			// },
		},
	}

	return f.pod, nil
}

const fbcCmdTemplate = `mkdir -p {{ .FBCDir }} && \
opm serve /configs -p {{ .GRPCPort }}
`

// getContainerCmd uses templating to construct the container command
// and throws error if unable to parse and execute the container command
func (f *FBCRegistryPod) getContainerCmd() (string, error) {
	var t *template.Template
	// create a custom dirname template function
	funcMap := template.FuncMap{
		"dirname": path.Dir,
	}

	// add the custom dirname template function to the
	// template's FuncMap and parse the cmdTemplate
	t = template.Must(template.New("cmd").Funcs(funcMap).Parse(fbcCmdTemplate))

	// execute the command by applying the parsed template to command
	// and write command output to out
	out := &bytes.Buffer{}
	if err := t.Execute(out, f); err != nil {
		return "", fmt.Errorf("parse container command: %w", err)
	}

	return out.String(), nil
}

func getConfigMaps(fbcContent, namespace string) ([]corev1.ConfigMap, error) {
	cms := []corev1.ConfigMap{}

	partitions, err := partitionFBC(fbcContent)
	if err != nil {
		return nil, fmt.Errorf("encountered an error getting partitions: %w", err)
	}

	// TODO: check if size of contents is to big and break into even smaller config maps
	for k, v := range partitions {
		cms = append(cms, corev1.ConfigMap{
			TypeMeta: metav1.TypeMeta{
				APIVersion: corev1.SchemeGroupVersion.String(),
				Kind:       "ConfigMap",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("operator-sdk-run-bundle-config-%s", k),
				Namespace: namespace,
			},
			Data: map[string]string{
				k: v,
			},
		})
	}

	return cms, nil
}

func partitionFBC(fbcContent string) (map[string]string, error) {
	partitions := map[string]string{}

	declcfg, err := unmarshalFBC(fbcContent)
	if err != nil {
		return nil, fmt.Errorf("encountered an error unmarshaling FBC contents: %w | %s", err, fbcContent)
	}

	for _, pack := range declcfg.Packages {
		packDecl := getDeclarativeConfigForPackage(pack, declcfg)
		contents, err := fbcutil.ValidateAndStringify(packDecl)
		if err != nil {
			return nil, fmt.Errorf("encountered an error partitioning the FBC: %w", err)
		}

		partitions[pack.Name] = contents
	}

	return partitions, nil
}

func getDeclarativeConfigForPackage(pack declarativeconfig.Package, declcfg *declarativeconfig.DeclarativeConfig) *declarativeconfig.DeclarativeConfig {
	packDeclCfg := &declarativeconfig.DeclarativeConfig{}
	packDeclCfg.Packages = append(packDeclCfg.Packages, pack)

	for _, channel := range declcfg.Channels {
		if channel.Package == pack.Name {
			packDeclCfg.Channels = append(packDeclCfg.Channels, channel)
		}
	}

	for _, bundle := range declcfg.Bundles {
		if bundle.Package == pack.Name {
			packDeclCfg.Bundles = append(packDeclCfg.Bundles, bundle)
		}
	}

	for _, meta := range declcfg.Others {
		if meta.Package == pack.Name {
			packDeclCfg.Others = append(packDeclCfg.Others, meta)
		}
	}

	return packDeclCfg
}

func unmarshalFBC(fbcContent string) (*declarativeconfig.DeclarativeConfig, error) {
	declcfg := &declarativeconfig.DeclarativeConfig{}

	decoder := yaml.NewDecoder(bytes.NewBufferString(fbcContent))
	decoder.KnownFields(true)

	for {
		helper := &FBCCatchAll{}
		err := decoder.Decode(helper)
		if err != nil {
			if err == io.EOF {
				break
			}

			return &declarativeconfig.DeclarativeConfig{}, fmt.Errorf("encountered an error when decoding FBC contents: %w", err)
		}

		properties, err := parseProperties(helper)
		if err != nil {
			return &declarativeconfig.DeclarativeConfig{}, fmt.Errorf("encountered an error when getting properties for a FBC item: %w", err)
		}
		switch helper.Schema {
		case "olm.package":
			pack := declarativeconfig.Package{
				Schema:     helper.Schema,
				Name:       helper.Name,
				Properties: properties,
				// TODO: Find out why this is not parsing correctly
				DefaultChannel: helper.DefaultChannel,
				Icon: &declarativeconfig.Icon{
					Data:      []byte(helper.Icon.Data),
					MediaType: helper.Icon.MediaType,
				},
				Description: helper.Description,
			}

			declcfg.Packages = append(declcfg.Packages, pack)

		case "olm.bundle":
			relatedImages := []declarativeconfig.RelatedImage{}
			for _, ri := range helper.RelatedImages {
				relatedImages = append(relatedImages, declarativeconfig.RelatedImage{
					Name:  ri.Name,
					Image: ri.Image,
				})
			}
			bundle := declarativeconfig.Bundle{
				Schema:        helper.Schema,
				Name:          helper.Name,
				Properties:    properties,
				Image:         helper.Image,
				RelatedImages: relatedImages,
				CsvJSON:       helper.CsvJSON,
				Objects:       helper.Objects,
				Package:       helper.Package,
			}

			declcfg.Bundles = append(declcfg.Bundles, bundle)
		case "olm.channel":
			entries := []declarativeconfig.ChannelEntry{}
			for _, entry := range helper.Entries {
				entries = append(entries, declarativeconfig.ChannelEntry{
					Name:      entry.Name,
					Replaces:  entry.Replaces,
					Skips:     entry.Skips,
					SkipRange: entry.SkipRange,
				})
			}
			channel := declarativeconfig.Channel{
				Schema:     helper.Schema,
				Name:       helper.Name,
				Properties: properties,
				Package:    helper.Package,
				Entries:    entries,
			}

			declcfg.Channels = append(declcfg.Channels, channel)
		}
	}

	return declcfg, nil
}

func parseProperties(helper *FBCCatchAll) ([]property.Property, error) {
	properties := []property.Property{}
	for _, prop := range helper.Properties {
		val, err := json.Marshal(prop.Value)
		if err != nil {
			return nil, fmt.Errorf("encountered an error when parsing properties: %w", err)
		}
		properties = append(properties, property.Property{
			Type:  prop.Type,
			Value: json.RawMessage(val),
		})
	}

	return properties, nil
}

type FBCCatchAll struct {
	// present in all
	Schema     string     `json:"schema" yaml:"schema"`
	Name       string     `json:"name" yaml:"name"`
	Properties []Property `json:"properties,omitempty" hash:"set" yaml:"properties,omitempty"`

	// Package
	DefaultChannel string `json:"defaultChannel" yaml:"defaultChannel"`
	Icon           Icon   `json:"icon,omitempty" yaml:"icon,omitempty"`
	Description    string `json:"description,omitempty" yaml:"description,omitempty"`

	// All but Package
	Package string `json:"package" yaml:"package"`

	// Channel
	Entries []ChannelEntry `json:"entries" yaml:"entries"`

	// Bundle
	Image         string         `json:"image" yaml:"image"`
	RelatedImages []RelatedImage `json:"relatedImages,omitempty" hash:"set" yaml:"relatedImages,omitempty"`
	CsvJSON       string         `json:"-" yaml:"-"`
	Objects       []string       `json:"-" yaml:"-"`
}

// Note: This is because the declcfg package doesn't have yaml definitions for their fields so we have to recreate the nested fields here.
type Property struct {
	Type  string      `json:"type" yaml:"type"`
	Value interface{} `json:"value" yaml:"value"`
}

type Icon struct {
	Data      string `json:"base64data" yaml:"base64data"`
	MediaType string `json:"mediatype" yaml:"mediatype"`
}

type ChannelEntry struct {
	Name      string   `json:"name" yaml:"name"`
	Replaces  string   `json:"replaces,omitempty" yaml:"replaces,omitempty"`
	Skips     []string `json:"skips,omitempty" yaml:"skips,omitempty"`
	SkipRange string   `json:"skipRange,omitempty" yaml:"skipRange,omitempty"`
}

type RelatedImage struct {
	Name  string `json:"name" yaml:"name"`
	Image string `json:"image" yaml:"image"`
}
