package cli

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/AlecAivazis/survey/v2"
	"github.com/stretchr/testify/assert"

	"sequin-cli/context"
)

var askOne = survey.AskOne

func TestCreateAction(t *testing.T) {
	// Setup
	cmd := &ctxCommand{
		name:        "test-context",
		description: "Test description",
		serverURL:   "http://test-server.com",
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
	assert.Equal(t, cmd.serverURL, savedContext.ServerURL)

	// Cleanup
	context.RemoveContext("test-context")
}

func TestListAction(t *testing.T) {
	// Setup
	testContexts := []context.Context{
		{Name: "context1", Description: "Description 1", ServerURL: "http://server1.com"},
		{Name: "context2", Description: "Description 2", ServerURL: "http://server2.com"},
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
		assert.Contains(t, output, ctx.ServerURL)
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
		ServerURL:   "http://test-server.com",
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
	assert.Contains(t, output, testContext.ServerURL)

	// Cleanup
	context.RemoveContext(testContext.Name)
}
