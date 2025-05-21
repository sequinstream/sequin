<script lang="ts">
  import { EditorView, basicSetup } from "codemirror";
  import { EditorState } from "@codemirror/state";
  import { elixir } from "codemirror-lang-elixir";
  import { keymap } from "@codemirror/view";
  import { indentWithTab } from "@codemirror/commands";
  import { autocompletion } from "@codemirror/autocomplete";
  import { Pencil, Save, Ban } from "lucide-svelte";

  export let form;
  export let formErrors;
  export let selectedMessage;

  let editorElement: HTMLElement;
  let editorView: EditorView | undefined;

  let isEditingField: boolean = false;

  export let field: "record" | "metadata" | "changes" | "action";
  // $: {
  //     let kind = if(field === "action") "dropdown" else "editor";
  // }

  // Add a function to create/update the editor view
  function updateMessageEditor(message: TestMessage) {
    // Destroy existing editor if it exists
    if (editorView) {
      editorView.destroy();
      editorView = null;
    }

    const editorState = EditorState.create({
      doc: message[field],
      extensions: [
        basicSetup,
        elixir(),
        autocompletion({ override: [] }),
        keymap.of([indentWithTab]),
      ],
    });

    editorView = new EditorView({
      state: editorState,
      parent: editorElement,
    });
  }

  $: updateMessageEditor(selectedMessage);
</script>

<div
  class="text-sm bg-slate-50 dark:bg-slate-800/50 p-3 rounded-md overflow-auto font-mono text-slate-700 dark:text-slate-300 select-text space-y-4 mb-3"
>
  <div class="font-semibold mb-1 flex w-full justify-between items-center">
    <span>{field}</span>
    {#if !isEditingField}
      <div>
        <button
          type="button"
          on:click={() => {
            isEditingField = true;
          }}
        >
          <Pencil
            class="h-4 w-4 ml-2 text-slate-500 hover:text-slate-700 cursor-pointer"
          />
        </button>
      </div>
    {:else}
      <div>
        <button
          type="button"
          on:click={() => {
            selectedMessage[field] = editorView.state.doc.toString();

            form.modified_test_messages[selectedMessage.id] = selectedMessage;

            isEditingField = false;
          }}
        >
          <Save
            class="h-4 w-4 ml-2 text-slate-500 hover:text-slate-700 cursor-pointer"
          />
        </button>

        <button
          type="button"
          on:click={() => {
            isEditingField = false;
          }}
        >
          <Ban
            class="h-4 w-4 ml-2 text-slate-500 hover:text-slate-700 cursor-pointer"
          />
        </button>
      </div>
    {/if}
  </div>
  <div hidden={!isEditingField} bind:this={editorElement} />
  <pre hidden={isEditingField}>{selectedMessage[field]}</pre>
  {#if formErrors?.modified_test_messages?.[selectedMessage.id]?.[field]}
    <p class="text-sm text-red-500 dark:text-red-400">
      {formErrors.modified_test_messages[selectedMessage.id][field].type}: {formErrors
        .modified_test_messages[selectedMessage.id][field].info.description}
    </p>
  {/if}
</div>
