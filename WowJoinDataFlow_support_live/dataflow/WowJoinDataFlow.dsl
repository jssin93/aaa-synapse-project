source(output(
		char as string,
		level as string,
		race as string,
		charclass as string,
		zone as string,
		guild as string,
		timestamp as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false) ~> wowlogsource
source(output(
		char as string,
		timestamp as string,
		churn as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false) ~> churnerssource
source(output(
		Zone_Name as string,
		Continent as string,
		Area as string,
		Zone as string,
		Subzone as string,
		Type as string,
		Size as string,
		Controlled as string,
		Min_req_level as string,
		Min_rec_level as string,
		Max_rec_level as string,
		Min_bot_level as string,
		Max_bot_level as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false) ~> zonesdataset
wowlogsource, churnerssource join(wowlogsource@char == churnerssource@char,
	joinType:'inner',
	broadcast: 'auto')~> idjoin
idjoin, zonesdataset join(wowlogsource@zone == Zone_Name,
	joinType:'inner',
	broadcast: 'auto')~> zonejoin
zonejoin select(mapColumn(
		IdentifierId = wowlogsource@char,
		level,
		race,
		charclass,
		zoneId = wowlogsource@zone,
		guild,
		timestamp = wowlogsource@timestamp,
		timestamp = churnerssource@timestamp,
		churn,
		Zone_Name,
		Continent,
		Area,
		Subzone,
		Type,
		Size,
		Controlled,
		Min_req_level,
		Min_rec_level,
		Max_rec_level,
		Min_bot_level,
		Max_bot_level
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> selectmapping
selectmapping derive(IdentifierId = regexReplace(regexReplace(IdentifierId, "IdentifierId\\(", ""), "\\)", ""),
		zoneId = regexReplace(regexReplace(zoneId, "ZoneidId\\(", ""), "\\)", "")) ~> extractvalue
extractvalue sink(input(
		col1 as integer
	),
	allowSchemaDrift: true,
	validateSchema: false,
	deletable:false,
	insertable:true,
	updateable:false,
	upsertable:false,
	recreate:true,
	format: 'table',
	staged: true,
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> sinkdataset