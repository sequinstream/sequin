package cli

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sequinstream/sequin/cli/context"
)

func TestCreateAction(t *testing.T) {
	// Setup
	cmd := &ctxCommand{
		name:        "test-context",
		description: "Test description",
		hostname:    "http://test-server.com",
		tls:         true,
		setDefault:  true,
	}

	// Redirect stdout to capture output
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Execute
	err := cmd.createAction(nil)

	// Restore stdout
	w.Close()
	os.Stdout = old

	// Capture the output
	var buf bytes.Buffer
	io.Copy(&buf, r)

	// Assert
	assert.NoError(t, err)
	assert.Contains(t, buf.String(), "Context 'test-context' created successfully.")

	// Verify the context was saved
	savedContext, err := context.LoadContext("test-context")
	assert.NoError(t, err)
	assert.Equal(t, cmd.name, savedContext.Name)
	assert.Equal(t, cmd.description, savedContext.Description)
	assert.Equal(t, cmd.hostname, savedContext.Hostname)
	assert.Equal(t, cmd.tls, savedContext.TLS)

	// Cleanup
	context.RemoveContext("test-context")
}

func TestListAction(t *testing.T) {
	// Setup
	testContexts := []context.Context{
		{Name: "context1", Description: "Description 1", Hostname: "http://server1.com"},
		{Name: "context2", Description: "Description 2", Hostname: "http://server2.com"},
	}

	for _, ctx := range testContexts {
		err := context.SaveContext(ctx)
		assert.NoError(t, err)
	}

	cmd := &ctxCommand{}

	// Redirect stdout to capture output
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Execute
	err := cmd.listAction(nil)

	// Restore stdout
	w.Close()
	os.Stdout = old

	// Capture the output
	var buf bytes.Buffer
	io.Copy(&buf, r)

	// Assert
	assert.NoError(t, err)
	output := buf.String()
	for _, ctx := range testContexts {
		assert.Contains(t, output, ctx.Name)
		assert.Contains(t, output, ctx.Description)
		assert.Contains(t, output, ctx.Hostname)
	}

	// Cleanup
	for _, ctx := range testContexts {
		context.RemoveContext(ctx.Name)
	}
}

func TestInfoActionWithoutSelection(t *testing.T) {
	// Setup
	testContext := context.Context{
		Name:        "test-context",
		Description: "Test Description",
		Hostname:    "http://test-server.com",
	}
	err := context.SaveContext(testContext)
	assert.NoError(t, err)

	cmd := &ctxCommand{name: "test-context"}

	// Redirect stdout to capture output
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Execute
	err = cmd.infoAction(nil)

	// Restore stdout
	w.Close()
	os.Stdout = old

	// Capture the output
	var buf bytes.Buffer
	io.Copy(&buf, r)

	// Assert
	assert.NoError(t, err)
	output := buf.String()
	assert.Contains(t, output, testContext.Name)
	assert.Contains(t, output, testContext.Description)
	assert.Contains(t, output, testContext.Hostname)

	// Cleanup
	context.RemoveContext(testContext.Name)
}
