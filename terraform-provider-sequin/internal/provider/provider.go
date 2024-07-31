// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/sequinstream/sequin-go"
)

type SequinProvider struct {
	version string
	client  sequin.SequinClientInterface
}

type SequinProviderModel struct {
	Endpoint types.String `tfsdk:"endpoint"`
	APIKey   types.String `tfsdk:"api_key"`
}

func NewSequinClient(endpoint, apiKey string) sequin.SequinClientInterface {
	return sequin.NewClient(endpoint, sequin.WithAPIKey(apiKey))
}

func New(version string, client sequin.SequinClientInterface) func() provider.Provider {
	return func() provider.Provider {
		return &SequinProvider{
			version: version,
			client:  client,
		}
	}
}

func (p *SequinProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "sequin"
	resp.Version = p.version
}

func (p *SequinProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"endpoint": schema.StringAttribute{
				MarkdownDescription: "API endpoint for Sequin",
				Required:            true,
			},
			"api_key": schema.StringAttribute{
				MarkdownDescription: "API key for authentication",
				Required:            true,
				Sensitive:           true,
			},
		},
	}
}

func (p *SequinProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var config SequinProviderModel
	diags := req.Config.Get(ctx, &config)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	if p.client == nil {
		p.client = NewSequinClient(config.Endpoint.ValueString(), config.APIKey.ValueString())
	}
	resp.DataSourceData = p.client
	resp.ResourceData = p.client
}

func (p *SequinProvider) Resources(_ context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewStreamResource,
		NewConsumerResource,
	}
}

func (p *SequinProvider) DataSources(_ context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{}
}
