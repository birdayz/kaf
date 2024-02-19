package main

import (
	"github.com/birdayz/kaf/pkg/config"
	kafv1 "github.com/birdayz/kaf/proto/gen/go/kaf/v1"
)

func main() {
	examples := []struct {
		filename string
		profile  *kafv1.Profile
	}{
		{
			"protobuf.yaml",
			&kafv1.Profile{
				Topics: []*kafv1.TopicConfig{
					{
						Name: "my-topic-a",
						Serde: &kafv1.TopicConfig_Serde{
							Kind: &kafv1.TopicConfig_Serde_Protobuf{
								Protobuf: &kafv1.TopicConfig_Protobuf{
									KeyType:      "google.protobuf.StringValue",
									ValueType:    "google.protobuf.StringValue",
									IncludePaths: []string{"./proto"},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, example := range examples {
		path := "./examples/profile/" + example.filename
		if err := config.Save(example.profile, path); err != nil {
			panic(err)
		}

		// Try to load it, make sure both ways work.

		_, err := config.Load(path)
		if err != nil {
			panic(err)
		}
	}
}
