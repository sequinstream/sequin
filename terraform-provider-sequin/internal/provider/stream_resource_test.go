package provider

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/sequinstream/sequin-go"
)

func TestAccStreamResource(t *testing.T) {
	mockClient := sequin.NewMockClient()
	resource.Test(t, resource.TestCase{
		PreCheck: func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: map[string]func() (tfprotov6.ProviderServer, error){
			"sequin": providerserver.NewProtocol6WithError(New("test", mockClient)()),
		},
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: testAccStreamResourceConfig("test-stream"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("sequin_stream.test", "name", "test-stream"),
					resource.TestCheckResourceAttrSet("sequin_stream.test", "id"),
				),
			},
			// ImportState testing
			{
				ResourceName:      "sequin_stream.test",
				ImportState:       true,
				ImportStateVerify: true,
			},
			// Update and Read testing
			{
				Config: testAccStreamResourceConfig("updated-test-stream"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("sequin_stream.test", "name", "updated-test-stream"),
				),
			},
			// Delete testing automatically occurs in TestCase
		},
	})
}

func testAccStreamResourceConfig(name string) string {
	return fmt.Sprintf(`
resource "sequin_stream" "test" {
  name = %[1]q
}
`, name)
}
