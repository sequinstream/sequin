defmodule Sequin.Test.UnboxedRepo.Migrations.CreateTestTables do
  @moduledoc false
  use Ecto.Migration

  def change do
    create table(:characters) do
      add :name, :text
      add :house, :text
      add :planet, :text
      add :is_active, :boolean
      add :tags, {:array, :text}
    end

    create table(:characters_ident_full) do
      add :name, :text
      add :house, :text
      add :planet, :text
      add :is_active, :boolean
      add :tags, {:array, :text}
    end

    execute "alter table characters_ident_full replica identity full"

    create table(:characters_2pk, primary_key: false) do
      add :id1, :serial, primary_key: true
      add :id2, :serial, primary_key: true
      add :name, :text
      add :house, :text
      add :planet, :text
    end

    execute "create publication characters_publication for table characters, characters_2pk, characters_ident_full",
            "drop publication characters_publication"
  end
end
