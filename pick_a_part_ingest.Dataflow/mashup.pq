[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Append"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Fabric.Warehouse([CreateNavigationProperties = false]){[workspaceId = "0cd3d9a9-0414-44eb-91ad-5516f97df911"]}[Data]{[warehouseId = "418ef95c-6a4a-4f2f-9028-82e3c1680a39"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "vehicle_id", DestinationColumnName = "vehicle_id"], [SourceColumnName = "site", DestinationColumnName = "site"], [SourceColumnName = "series_url", DestinationColumnName = "series_url"], [SourceColumnName = "details.vehicle_ref", DestinationColumnName = "details.vehicle_ref"], [SourceColumnName = "details.acq", DestinationColumnName = "details.acq"], [SourceColumnName = "details.yard_location", DestinationColumnName = "details.yard_location"], [SourceColumnName = "details.make_model", DestinationColumnName = "details.make_model"], [SourceColumnName = "details.engine", DestinationColumnName = "details.engine"], [SourceColumnName = "details.transmission", DestinationColumnName = "details.transmission"], [SourceColumnName = "details.year", DestinationColumnName = "details.year"], [SourceColumnName = "details.odometer", DestinationColumnName = "details.odometer"], [SourceColumnName = "details.date_in_stock", DestinationColumnName = "details.date_in_stock"], [SourceColumnName = "details.source_url", DestinationColumnName = "details.source_url"], [SourceColumnName = "sold_parts", DestinationColumnName = "sold_parts"], [SourceColumnName = "run_id", DestinationColumnName = "run_id"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared raw = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0cd3d9a9-0414-44eb-91ad-5516f97df911"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a743e7da-a128-4060-a81f-4d6844a04abd"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "pickapart"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "raw"]}[Content],
  #"Navigation 5" = #"Navigation 4"{[Name = "vehicle"]}[Content],
  #"Filtered rows" = Table.SelectRows(#"Navigation 5", each ([Extension] = "")),
  #"Expanded Content" = Table.ExpandTableColumn(#"Filtered rows", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes.1", "Folder Path.1"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Content", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content.1])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Removed columns" = Table.RemoveColumns(#"Expanded table column", {"scraped_at", "details.description"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns", {{"vehicle_id", type text}, {"site", type text}, {"series_url", type text}, {"details.vehicle_ref", type text}, {"details.acq", type text}, {"details.yard_location", type text}, {"details.make_model", type text}, {"details.engine", type text}, {"details.transmission", type text}, {"details.year", type text}, {"details.odometer", type text}, {"details.date_in_stock", type text}, {"details.source_url", type text}, {"sold_parts", type text}, {"run_id", type text}})
in
  #"Changed column type";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0cd3d9a9-0414-44eb-91ad-5516f97df911"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a743e7da-a128-4060-a81f-4d6844a04abd"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "pickapart"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "raw"]}[Content],
  #"Navigation 5" = #"Navigation 4"{[Name = "vehicle"]}[Content],
  #"Expanded Content" = Table.ExpandTableColumn(#"Navigation 5", "Content", {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}, {"Content.1", "Name.1", "Extension.1", "Date accessed.1", "Date modified.1", "Date created.1", "Attributes.1", "Folder Path.1"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Content", each [Attributes]?[Hidden]? <> true),
  #"Navigation 6" = #"Filtered hidden files"{0}[Content.1]
in
  #"Navigation 6";
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Json.Document(Parameter),
  #"Converted to table" = Table.FromRecords({Source}),
  #"Expanded details" = Table.ExpandRecordColumn(#"Converted to table", "details", {"vehicle_ref", "acq", "yard_location", "make_model", "engine", "transmission", "year", "odometer", "date_in_stock", "description", "source_url"}, {"details.vehicle_ref", "details.acq", "details.yard_location", "details.make_model", "details.engine", "details.transmission", "details.year", "details.odometer", "details.date_in_stock", "details.description", "details.source_url"}),
  #"Expanded sold_parts" = Table.ExpandListColumn(#"Expanded details", "sold_parts")
in
  #"Expanded sold_parts";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Json.Document(Parameter),
  #"Converted to table" = Table.FromRecords({Source}),
  #"Expanded details" = Table.ExpandRecordColumn(#"Converted to table", "details", {"vehicle_ref", "acq", "yard_location", "make_model", "engine", "transmission", "year", "odometer", "date_in_stock", "description", "source_url"}, {"details.vehicle_ref", "details.acq", "details.yard_location", "details.make_model", "details.engine", "details.transmission", "details.year", "details.odometer", "details.date_in_stock", "details.description", "details.source_url"}),
  #"Expanded sold_parts" = Table.ExpandListColumn(#"Expanded details", "sold_parts")
in
  #"Expanded sold_parts";
shared raw_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "0cd3d9a9-0414-44eb-91ad-5516f97df911"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "418ef95c-6a4a-4f2f-9028-82e3c1680a39"]}[Data],
  TableNavigation = Navigation_2{[Item = "raw_vehicle", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
