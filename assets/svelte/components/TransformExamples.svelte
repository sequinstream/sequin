<script lang="ts">
  export let type: "record" | "change";
  export let transform: "none" | "record_only" | "code";
  export let operation: "insert" | "update" | "delete" | "read" = "insert";

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
      commit_timestamp: "2023-10-15T14:30:00Z",
      commit_lsn: 123456789,
      commit_idx: 1,
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
        commit_lsn: 123456790,
        commit_idx: 1,
        consumer: {
          id: "e2f9a3b1-7c6d-4b5a-9f8e-1d2c3b4a5e6f",
          name: "hr_updates_consumer",
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
        commit_lsn: 123456791,
        commit_idx: 1,
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
        commit_lsn: 123456792,
        commit_idx: 1,
        consumer: {
          id: "a1b2c3d4-e5f6-4a5b-8c7d-9e0f1a2b3c4d",
          name: "employee_departures_consumer",
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
        commit_lsn: 123456793,
        commit_idx: 1,
        consumer: {
          id: "e2f9a3b1-7c6d-4b5a-9f8e-1d2c3b4a5e6f",
          name: "hr_updates_consumer",
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
    },
  };

  $: transformedExample =
    transform === "code"
      ? "Coming soon! You'll be able to write custom code to transform your messages."
      : transform === "record_only"
        ? type === "record"
          ? recordExample.record
          : changeExamples[operation].record
        : type === "record"
          ? recordExample
          : changeExamples[operation];
</script>

{#if transform === "code"}
  <div class="space-y-2">
    <p>
      Coming soon! You'll be able to write custom code to transform your
      messages.
    </p>
  </div>
{:else if type === "record"}
  <div class="space-y-2">
    <p>
      {#if transform === "record_only"}
        When using "Record only" transform, you'll receive just the record data
        without the metadata.
      {:else}
        When using no transform, you'll receive the full message including
        metadata.
      {/if}
    </p>
    <pre class="bg-gray-100 p-4 rounded-md overflow-x-auto"><code
        >{JSON.stringify(transformedExample, null, 2).trim()}</code
      ></pre>
  </div>
{:else}
  <div class="space-y-2">
    <p>
      {#if transform === "record_only"}
        When using "Record only" transform, you'll receive just the record data
        without the metadata.
      {:else}
        When using no transform, you'll receive the full message including
        metadata.
      {/if}
    </p>
    <pre class="bg-gray-100 p-4 rounded-md overflow-x-auto"><code
        >{JSON.stringify(transformedExample, null, 2).trim()}</code
      ></pre>
  </div>
{/if}
