package provider

import (
	"context"
	"os"
	"terraform-provider-sequin/internal/sequin"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// Ensure the implementation satisfies the expected interfaces.
var (
    _ provider.Provider = &sequinProvider{}
)

// New is a helper function to simplify provider server and testing implementation.
func New(version string) func() provider.Provider {
    return func() provider.Provider {
        return &sequinProvider{
            Endpoint: types.StringValue(""),
            APIKey: types.StringValue(""),
        }
    }
}

// sequinProvider is the provider implementation.
type sequinProvider struct {
    // version is set to the provider version on release, "dev" when the
    // provider is built and ran locally, and "test" when running acceptance
    // testing.
	Endpoint types.String `tfsdk:"endpoint"`
	APIKey   types.String `tfsdk:"api_key"`
}

// Metadata returns the provider type name.
func (p *sequinProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
    resp.TypeName = "sequin"
}

// Schema defines the provider-level schema for configuration data.
func (p *sequinProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
    resp.Schema = schema.Schema{
        Attributes: map[string]schema.Attribute{
            "endpoint": schema.StringAttribute{
                Optional: true,
            },
            "api_key": schema.StringAttribute{
                Required: true,
            },
        },
    }
}


func (p *sequinProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
    // Retrieve provider data from configuration
    var config sequinProvider
    diags := req.Config.Get(ctx, &config)
    resp.Diagnostics.Append(diags...)
    if resp.Diagnostics.HasError() {
        return
    }

    // If practitioner provided a configuration value for any of the
    // attributes, it must be a known value.

    if config.Endpoint.IsUnknown() {
        resp.Diagnostics.AddAttributeError(
            path.Root("endpoint"),
            "Unknown Sequin API Host",
            "The provider cannot create the Sequin API client as there is an unknown configuration value for the Sequin API host. "+
                "Either target apply the source of the value first, set the value statically in the configuration, or use the HASHICUPS_HOST environment variable.",
        )
    }

    if config.APIKey.IsUnknown() {
        resp.Diagnostics.AddAttributeError(
            path.Root("api_key"),
            "Unknown Sequin API Key",
            "The provider cannot create the Sequin API client as there is an unknown configuration value for the Sequin API key. "+
                "Either target apply the source of the value first, set the value statically in the configuration, or use the HASHICUPS_USERNAME environment variable.",
        )
    }


    if resp.Diagnostics.HasError() {
        return
    }

    // Default values to environment variables, but override
    // with Terraform configuration value if set.

    host := os.Getenv("SEQUIN_HOST")
    apiKey := os.Getenv("SEQUIN_API_KEY")

    if !config.Endpoint.IsNull() {
        host = config.Endpoint.ValueString()
    }

    if !config.APIKey.IsNull() {
        apiKey = config.APIKey.ValueString()
    }

    // If any of the expected configurations are missing, return
    // errors with provider-specific guidance.

    if host == "" {
        resp.Diagnostics.AddAttributeError(
            path.Root("endpoint"),
            "Missing Sequin API Host",
            "The provider cannot create the Sequin API client as there is a missing or empty value for the Sequin API host. "+
                "Set the host value in the configuration or use the SEQUIN_HOST environment variable. "+
                "If either is already set, ensure the value is not empty.",
        )
    }

    if apiKey == "" {
        resp.Diagnostics.AddAttributeError(
            path.Root("api_key"),
            "Missing Sequin API Key",
            "The provider cannot create the Sequin API client as there is a missing or empty value for the Sequin API key. "+
                "Set the api_key value in the configuration or use the SEQUIN_API_KEY environment variable. "+
                "If either is already set, ensure the value is not empty.",
        )
    }

    if resp.Diagnostics.HasError() {
        return
    }

    // Create a new HashiCups client using the configuration values
    client, err := sequin.NewClient(&host, &apiKey)
    if err != nil {
        resp.Diagnostics.AddError(
            "Unable to Create Sequin API Client",
            "An unexpected error occurred when creating the Sequin API client. "+
                "If the error is not clear, please contact the provider developers.\n\n"+
                "Sequin Client Error: "+err.Error(),
        )
        return
    }

    // Make the HashiCups client available during DataSource and Resource
    // type Configure methods.
    resp.DataSourceData = client
    resp.ResourceData = client
}


// DataSources defines the data sources implemented in the provider.
func (p *sequinProvider) DataSources(_ context.Context) []func() datasource.DataSource {
    return []func() datasource.DataSource {
		NewDatabaseDataSource,
	}
}


// Resources defines the resources implemented in the provider.
func (p *sequinProvider) Resources(_ context.Context) []func() resource.Resource {
    return nil
}
