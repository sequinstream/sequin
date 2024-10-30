package provider

import (
	"context"
	"fmt"
	"terraform-provider-sequin/internal/sequin"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ datasource.DataSource              = &databaseDataSource{}
	_ datasource.DataSourceWithConfigure = &databaseDataSource{}
)

func NewDatabaseDataSource() datasource.DataSource {
	return &databaseDataSource{}
}

type databaseDataSource struct{
	client *sequin.Client
}
// coffeesDataSourceModel maps the data source schema data.
type databaseDataSourceModel struct {
    Databases []databaseModel `tfsdk:"databases"`
}

// databaseModel maps database schema data.
type databaseModel struct {
    ID          types.String              `tfsdk:"id"`
    Name        types.String              `tfsdk:"name"`
    Hostname    types.String              `tfsdk:"hostname"`
	Database    types.String              `tfsdk:"database"`
    PoolSize    types.Int64               `tfsdk:"pool_size"`
    Port        types.Int64               `tfsdk:"port"`
	SSL         types.Bool                `tfsdk:"ssl"`
    Username    types.String              `tfsdk:"username"`
    Ipv6        types.Bool                `tfsdk:"ipv6"`
    UseLocalTunnel types.Bool             `tfsdk:"use_local_tunnel"`
}

func (d *databaseDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_database"
}
// Configure adds the provider configured client to the data source.
func (d *databaseDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	// Add a nil check when handling ProviderData because Terraform
	// sets that data after it calls the ConfigureProvider RPC.
	if req.ProviderData == nil {
	  return
	}
  
	client, ok := req.ProviderData.(*sequin.Client)
	if !ok {
	  resp.Diagnostics.AddError(
		"Unexpected Data Source Configure Type",
		fmt.Sprintf("Expected *sequin.Client, got: %T. Please report this issue to the provider developers.", req.ProviderData),
	  )
  
	  return
	}
  
	d.client = client
}

// Schema defines the schema for the data source.
func (d *databaseDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"databases": schema.ListNestedAttribute{
				Computed: true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"id": schema.StringAttribute{
							Computed: true,
						},
						"database": schema.StringAttribute{
							Computed: true,
						},
						"hostname": schema.StringAttribute{
							Computed: true,
						},
						"pool_size": schema.Int64Attribute{
							Computed: true,
						},
						"port": schema.Int64Attribute{
							Computed: true,
						},
						"name": schema.StringAttribute{
							Computed: true,
						},
						"ssl": schema.BoolAttribute{
							Computed: true,
						},
						"username": schema.StringAttribute{
							Computed: true,
						},
						"ipv6": schema.BoolAttribute{
							Computed: true,
						},
						"use_local_tunnel": schema.BoolAttribute{
							Computed: true,
						},
					},
				},
			},
		},
	}
}

// Read refreshes the Terraform state with the latest data.
func (d *databaseDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
    var state databaseDataSourceModel

    databases, err := d.client.GetDatabases()
    if err != nil {
      resp.Diagnostics.AddError(
        "Unable to Read Sequin Databases",
        err.Error(),
      )
      return
    }

    // Map response body to model
    for _, database := range databases {
      databaseState := databaseModel{
        ID:          types.StringValue(database.ID),
        Name:        types.StringValue(database.Name),
        Hostname:    types.StringValue(database.Hostname),
        Database:    types.StringValue(database.Database),
        PoolSize:    types.Int64Value(int64(database.PoolSize)),
        Port:        types.Int64Value(int64(database.Port)),
        SSL:         types.BoolValue(database.SSL),
        Username:    types.StringValue(database.Username),
        Ipv6:        types.BoolValue(database.Ipv6),
        UseLocalTunnel: types.BoolValue(database.UseLocalTunnel),
      }

      state.Databases = append(state.Databases, databaseState)
    }

    // Set state
    diags := resp.State.Set(ctx, &state)
    resp.Diagnostics.Append(diags...)
    if resp.Diagnostics.HasError() {
      return
    }
}
