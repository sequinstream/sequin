// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/function"
)

var (
	_ function.Function = ExampleFunction{}
)

func NewExampleFunction() function.Function {
	return ExampleFunction{}
}

type ExampleFunction struct{}

func (r ExampleFunction) Metadata(_ context.Context, req function.MetadataRequest, resp *function.MetadataResponse) {
	resp.Name = "example"
}

func (r ExampleFunction) Definition(_ context.Context, _ function.DefinitionRequest, resp *function.DefinitionResponse) {
	resp.Definition = function.Definition{
		Summary:             "Example function",
		MarkdownDescription: "Echoes given argument as result",
		Parameters: []function.Parameter{
			function.StringParameter{
				Name:                "input",
				MarkdownDescription: "String to echo",
			},
		},
		Return: function.StringReturn{},
	}
}

func (r ExampleFunction) Run(ctx context.Context, req function.RunRequest, resp *function.RunResponse) {
	var data string

	resp.Error = function.ConcatFuncErrors(req.Arguments.Get(ctx, &data))

	if resp.Error != nil {
		return
	}

	resp.Error = function.ConcatFuncErrors(resp.Result.Set(ctx, data))
}
