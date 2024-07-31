package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/sequinstream/sequin-go"
)

var _ resource.Resource = &ConsumerResource{}
var _ resource.ResourceWithImportState = &ConsumerResource{}

func NewConsumerResource() resource.Resource {
	return &ConsumerResource{}
}

type ConsumerResource struct {
	client sequin.SequinClientInterface
}

type ConsumerResourceModel struct {
	ID               types.String `tfsdk:"id"`
	StreamID         types.String `tfsdk:"stream_id"`
	Name             types.String `tfsdk:"name"`
	FilterKeyPattern types.String `tfsdk:"filter_key_pattern"`
	AckWaitMS        types.Int64  `tfsdk:"ack_wait_ms"`
	MaxAckPending    types.Int64  `tfsdk:"max_ack_pending"`
	MaxDeliver       types.Int64  `tfsdk:"max_deliver"`
	Kind             types.String `tfsdk:"kind"`
}

func (r *ConsumerResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_consumer"
}

func (r *ConsumerResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Manages a Sequin Consumer",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Consumer identifier",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"stream_id": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "ID of the stream the consumer is attached to",
			},
			"name": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "Name of the consumer",
			},
			"filter_key_pattern": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "Pattern used to filter messages for this consumer",
			},
			"ack_wait_ms": schema.Int64Attribute{
				Optional:            true,
				MarkdownDescription: "Acknowledgement wait time in milliseconds",
			},
			"max_ack_pending": schema.Int64Attribute{
				Optional:            true,
				MarkdownDescription: "Maximum number of pending acknowledgements",
			},
			"max_deliver": schema.Int64Attribute{
				Optional:            true,
				MarkdownDescription: "Maximum number of delivery attempts",
			},
			"kind": schema.StringAttribute{
				Optional:            true,
				MarkdownDescription: "Kind of consumer (pull or push)",
			},
		},
	}
}

func (r *ConsumerResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(sequin.SequinClientInterface)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf("Expected *SequinClient, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
		return
	}

	r.client = client
}

func (r *ConsumerResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data ConsumerResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	options := &sequin.CreateConsumerOptions{
		AckWaitMS:     int(data.AckWaitMS.ValueInt64()),
		MaxAckPending: int(data.MaxAckPending.ValueInt64()),
		MaxDeliver:    int(data.MaxDeliver.ValueInt64()),
		Kind:          data.Kind.ValueString(),
	}

	consumer, err := r.client.CreateConsumer(
		data.StreamID.ValueString(),
		data.Name.ValueString(),
		data.FilterKeyPattern.ValueString(),
		options,
	)
	if err != nil {
		resp.Diagnostics.AddError("Client Error", fmt.Sprintf("Unable to create consumer, got error: %s", err))
		return
	}

	data.ID = types.StringValue(consumer.ID)
	data.AckWaitMS = types.Int64Value(int64(consumer.AckWaitMS))
	data.MaxAckPending = types.Int64Value(int64(consumer.MaxAckPending))
	data.MaxDeliver = types.Int64Value(int64(consumer.MaxDeliver))
	data.Kind = types.StringValue(consumer.Kind)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *ConsumerResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data ConsumerResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	consumer, err := r.client.GetConsumer(data.StreamID.ValueString(), data.ID.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Client Error", fmt.Sprintf("Unable to read consumer, got error: %s", err))
		return
	}

	data.Name = types.StringValue(consumer.Name)
	data.FilterKeyPattern = types.StringValue(consumer.FilterKeyPattern)
	data.AckWaitMS = types.Int64Value(int64(consumer.AckWaitMS))
	data.MaxAckPending = types.Int64Value(int64(consumer.MaxAckPending))
	data.MaxDeliver = types.Int64Value(int64(consumer.MaxDeliver))
	data.Kind = types.StringValue(consumer.Kind)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *ConsumerResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data ConsumerResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	options := &sequin.UpdateConsumerOptions{
		AckWaitMS:     sequin.IntPtr(int(data.AckWaitMS.ValueInt64())),
		MaxAckPending: sequin.IntPtr(int(data.MaxAckPending.ValueInt64())),
		MaxDeliver:    sequin.IntPtr(int(data.MaxDeliver.ValueInt64())),
		Kind:          sequin.StringPtr(data.Kind.ValueString()),
	}

	consumer, err := r.client.UpdateConsumer(data.StreamID.ValueString(), data.ID.ValueString(), options)
	if err != nil {
		resp.Diagnostics.AddError("Client Error", fmt.Sprintf("Unable to update consumer, got error: %s", err))
		return
	}

	data.AckWaitMS = types.Int64Value(int64(consumer.AckWaitMS))
	data.MaxAckPending = types.Int64Value(int64(consumer.MaxAckPending))
	data.MaxDeliver = types.Int64Value(int64(consumer.MaxDeliver))
	data.Kind = types.StringValue(consumer.Kind)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *ConsumerResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data ConsumerResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	_, err := r.client.DeleteConsumer(data.StreamID.ValueString(), data.ID.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Client Error", fmt.Sprintf("Unable to delete consumer, got error: %s", err))
		return
	}
}

func (r *ConsumerResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
