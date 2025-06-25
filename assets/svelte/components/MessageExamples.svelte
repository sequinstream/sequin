<script lang="ts">
  import * as Tabs from "$lib/components/ui/tabs";

  export let type: "record" | "change";

  const recordExample = {
    record: {
      id: 1,
      name: "John Smith",
      title: "Senior Developer",
      salary: 120000,
      is_manager: false,
    },
    metadata: {
      table_schema: "public",
      table_name: "employees",
      database_name: "myapp-prod",
      consumer: {
        id: "f47ac10b-58cc-4372-a567-0e02b2c3d479",
        name: "employee_updates_consumer",
        annotations: { "my-custom-key": "my-custom-value" },
      },
      database: {
        id: "12345678-9abc-def0-1234-56789abcdef0",
        name: "myapp-prod",
        annotations: { "my-custom-key": "my-custom-value" },
        database: "myapp-prod",
        hostname: "db.example.com",
      },
    },
  };

  const changeExamples = {
    insert: {
      record: {
        id: 2,
        name: "Sarah Johnson",
        title: "Product Manager",
        salary: 135000,
        is_manager: true,
      },
      changes: null,
      action: "insert",
      metadata: {
        table_schema: "public",
        table_name: "employees",
        database_name: "myapp-prod",
        commit_timestamp: "2023-10-15T14:30:00Z",
        consumer: {
          id: "e2f9a3b1-7c6d-4b5a-9f8e-1d2c3b4a5e6f",
          name: "hr_updates_consumer",
        },
      },
    },
    update: {
      record: {
        id: 1,
        name: "John Smith",
        title: "Engineering Manager",
        salary: 150000,
        is_manager: true,
      },
      changes: {
        title: "Senior Developer",
        salary: 120000,
        is_manager: false,
      },
      action: "update",
      metadata: {
        table_schema: "public",
        table_name: "employees",
        database_name: "myapp-prod",
        commit_timestamp: "2023-10-16T09:45:00Z",
        consumer: {
          id: "f47ac10b-58cc-4372-a567-0e02b2c3d479",
          name: "employee_updates_consumer",
        },
      },
    },
    delete: {
      record: {
        id: 3,
        name: "Michael Brown",
        title: "Software Engineer",
        salary: 110000,
        is_manager: false,
      },
      changes: null,
      action: "delete",
      metadata: {
        table_schema: "public",
        table_name: "employees",
        database_name: "myapp-prod",
        commit_timestamp: "2023-10-17T18:20:00Z",
        consumer: {
          id: "a1b2c3d4-e5f6-4a5b-8c7d-9e0f1a2b3c4d",
          name: "employee_departures_consumer",
        },
      },
    },
    read: {
      record: {
        id: 2,
        name: "Sarah Johnson",
        title: "Product Manager",
        salary: 135000,
        is_manager: true,
      },
      changes: null,
      action: "read",
      metadata: {
        table_schema: "public",
        table_name: "employees",
        database_name: "myapp-prod",
        commit_timestamp: "2023-10-15T14:30:00Z",
        consumer: {
          id: "e2f9a3b1-7c6d-4b5a-9f8e-1d2c3b4a5e6f",
          name: "hr_updates_consumer",
        },
      },
    },
  };
</script>

{#if type === "record"}
  <div class="space-y-2">
    <p>
      A row message contains the latest version of a row. Sequin sends a row
      message whenever a row is inserted, updated, or backfilled.
    </p>
    <p>
      In contrast to change messages, row messages do not contain
      <code>changes</code> or <code>action</code> fields.
    </p>
    <pre class="bg-gray-100 p-4 rounded-md overflow-x-auto"><code
        >{JSON.stringify(recordExample, null, 2).trim()}</code
      ></pre>
  </div>
{:else}
  <Tabs.Root value="insert" class="w-full mb-4">
    <Tabs.List class="grid grid-cols-4 mb-4">
      <Tabs.Trigger value="insert" class="w-full">Insert</Tabs.Trigger>
      <Tabs.Trigger value="update" class="w-full">Update</Tabs.Trigger>
      <Tabs.Trigger value="delete" class="w-full">Delete</Tabs.Trigger>
      <Tabs.Trigger value="read" class="w-full">Read</Tabs.Trigger>
    </Tabs.List>
    <Tabs.Content value="insert" class="space-y-2">
      <p>Sequin emits an <code>insert</code> message when a row is inserted.</p>
      <pre class="bg-gray-100 p-4 rounded-md overflow-x-auto"><code
          >{JSON.stringify(changeExamples.insert, null, 2).trim()}</code
        ></pre>
    </Tabs.Content>
    <Tabs.Content value="update" class="space-y-2">
      <p>
        Sequin emits an <code>update</code> message when a row is updated. The
        <code>changes</code> field contains the previous values of the updated fields.
      </p>
      <pre class="bg-gray-100 p-4 rounded-md overflow-x-auto"><code
          >{JSON.stringify(changeExamples.update, null, 2).trim()}</code
        ></pre>
    </Tabs.Content>
    <Tabs.Content value="delete" class="space-y-2">
      <p>
        Sequin emits a <code>delete</code> message when a row is deleted. The
        <code>record</code> field contains the row's values before deletion.
      </p>
      <pre class="bg-gray-100 p-4 rounded-md overflow-x-auto"><code
          >{JSON.stringify(changeExamples.delete, null, 2).trim()}</code
        ></pre>
    </Tabs.Content>
    <Tabs.Content value="read" class="space-y-2">
      <p>
        Sequin emits a <code>read</code> message when a row is read during a backfill.
      </p>
      <pre class="bg-gray-100 p-4 rounded-md overflow-x-auto"><code
          >{JSON.stringify(changeExamples.read, null, 2).trim()}</code
        ></pre>
    </Tabs.Content>
  </Tabs.Root>
{/if}
