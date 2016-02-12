Create Table ItemGeo (
	ItemID int NOT NULL,
	GeoPosition GEOMETRY NOT NULL,
	Primary Key(ItemID),
	SPATIAL INDEX(GeoPosition)
)ENGINE=MyISAM;

Insert Into ItemGeo(ItemID, GeoPosition)
Select ItemID, Point(Latitude, Longitude)
From ItemPosition;