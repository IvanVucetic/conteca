import csv
import psycopg2 #postgres api

# create connection, connect
try:
	conn = psycopg2.connect("host='localhost' dbname='vuceta' user='vuceta' password='q'")
except:
	print "failed to connect"

# create cursor that lets us work with data in DB
try:
	cursor = conn.cursor()
except:
	print "cursor fail"

# drop table official_meta_full so we can insert new data
cursor.execute("DROP TABLE official_meta_full;")

# recreate the table
create_statement = """
	CREATE TABLE official_meta_full (
		recordId serial PRIMARY KEY,
		sceneID character(21) NOT NULL,
		sensor character(8),
		acquisitionDate date,
		dateUpdated date,
		browseAvailable character(1),
		browseURL text,
		path integer,
		row integer,
		upperLeftCornerLatitude float, 
		upperLeftCornerLongitude float,
		upperRightCornerLatitude float,
		upperRightCornerLongitude float,
		lowerLeftCornerLatitude float,
		lowerLeftCornerLongitude float,
		lowerRightCornerLatitude float,
		lowerRightCornerLongitude float,
		sceneCenterLatitude float,
		sceneCenterLongitude float,
		cloudCover float,
		cloudCoverFull float,
		FULL_UL_QUAD_CCA varchar(1),
		FULL_UR_QUAD_CCA varchar(1),
		FULL_LL_QUAD_CCA varchar(1),
		FULL_LR_QUAD_CCA varchar(1),
		dayOrNight varchar(5),
		flightPath  varchar(1),
		sunElevation float,
		sunAzimuth float,
		receivingStation varchar(10),
		sceneStartTime text,
		sceneStopTime text,
		lookAngle varchar(1),
		imageQuality1 integer,
		imageQuality2 varchar(1), 
		gainBand1 varchar(1),
		gainBand2 varchar(1),
		gainBand3 varchar(1),
		gainBand4 varchar(1),
		gainBand5 varchar(1),
		gainBand6H varchar(1),
		gainBand6L varchar(1),
		gainBand7 varchar(1),
		gainBand8 varchar(1),
		gainChangeBand1 varchar(1),
		gainChangeBand2 varchar(1),
		gainChangeBand3 varchar(1),
		gainChangeBand4 varchar(1),
		gainChangeBand5 varchar(1),
		gainChangeBand6H varchar(1),
		gainChangeBand6L varchar(1),
		gainChangeBand7 varchar(1),
		gainChangeBand8 varchar(1),
		satelliteNumber varchar(1),
		DATA_TYPE_L1  varchar(10),
		cartURL text,
		DATE_ACQUIRED_GAP_FILL varchar(1),
		DATA_TYPE_L0RP varchar(1),
		DATUM varchar(1),
		ELEVATION_SOURCE varchar(1),
		ELLIPSOID varchar(1),
		EPHEMERIS_TYPE varchar(1),
		FALSE_EASTING varchar(1),
		FALSE_NORTHING varchar(1),
		GAP_FILL varchar(1),
		GROUND_CONTROL_POINTS_MODEL integer,
		GROUND_CONTROL_POINTS_VERIFY varchar(1),
		GEOMETRIC_RMSE_MODEL varchar(1),
		GEOMETRIC_RMSE_MODEL_X float,
		GEOMETRIC_RMSE_MODEL_Y float,
		GEOMETRIC_RMSE_VERIFY varchar(1),
		GRID_CELL_SIZE_PANCHROMATIC varchar(1),
		GRID_CELL_SIZE_REFLECTIVE varchar(1),
		GRID_CELL_SIZE_THERMAL varchar(1),
		MAP_PROJECTION_L1 varchar(1),
		MAP_PROJECTION_L0RA varchar(1),
		ORIENTATION varchar(1),
		OUTPUT_FORMAT varchar(1),
		PANCHROMATIC_LINES varchar(1),
		PANCHROMATIC_SAMPLES varchar(1),
		L1_AVAILABLE varchar(1),
		REFLECTIVE_LINES varchar(1),
		REFLECTIVE_SAMPLES varchar(1),
		RESAMPLING_OPTION varchar(1),
		SCAN_GAP_INTERPOLATION varchar(1),
		THERMAL_LINES varchar(1),
		THERMAL_SAMPLES varchar(1),
		TRUE_SCALE_LAT varchar(1),
		UTM_ZONE varchar(1),
		VERTICAL_LON_FROM_POLE varchar(1),
		PRESENT_BAND_1 varchar(1),
		PRESENT_BAND_2 varchar(1),
		PRESENT_BAND_3 varchar(1),
		PRESENT_BAND_4 varchar(1),
		PRESENT_BAND_5 varchar(1),
		PRESENT_BAND_6 varchar(1),
		PRESENT_BAND_7 varchar(1),
		PRESENT_BAND_8 varchar(1),
		NADIR_OFFNADIR  varchar(8),
		the_geom geometry
	)
	"""
cursor.execute(create_statement)


# open csv with metadata
csvfile = open('scene-list/LANDSAT_8.csv', 'rb')

# import csv (with header) into db:
copy_statement = """
	COPY official_meta_full(sceneID,sensor,acquisitionDate,dateUpdated,browseAvailable,browseURL,path,row,upperLeftCornerLatitude,upperLeftCornerLongitude,upperRightCornerLatitude,upperRightCornerLongitude,lowerLeftCornerLatitude,lowerLeftCornerLongitude,lowerRightCornerLatitude,lowerRightCornerLongitude,sceneCenterLatitude,sceneCenterLongitude,cloudCover,cloudCoverFull,FULL_UL_QUAD_CCA,FULL_UR_QUAD_CCA,FULL_LL_QUAD_CCA,FULL_LR_QUAD_CCA,dayOrNight,flightPath,sunElevation,sunAzimuth,receivingStation,sceneStartTime,sceneStopTime,lookAngle,imageQuality1,imageQuality2,gainBand1,gainBand2,gainBand3,gainBand4,gainBand5,gainBand6H,gainBand6L,gainBand7,gainBand8,gainChangeBand1,gainChangeBand2,gainChangeBand3,gainChangeBand4,gainChangeBand5,gainChangeBand6H,gainChangeBand6L,gainChangeBand7,gainChangeBand8,satelliteNumber,DATA_TYPE_L1,cartURL,DATE_ACQUIRED_GAP_FILL,DATA_TYPE_L0RP,DATUM,ELEVATION_SOURCE,ELLIPSOID,EPHEMERIS_TYPE,FALSE_EASTING,FALSE_NORTHING,GAP_FILL,GROUND_CONTROL_POINTS_MODEL,GROUND_CONTROL_POINTS_VERIFY,GEOMETRIC_RMSE_MODEL,GEOMETRIC_RMSE_MODEL_X,GEOMETRIC_RMSE_MODEL_Y,GEOMETRIC_RMSE_VERIFY,GRID_CELL_SIZE_PANCHROMATIC,GRID_CELL_SIZE_REFLECTIVE,GRID_CELL_SIZE_THERMAL,MAP_PROJECTION_L1,MAP_PROJECTION_L0RA,ORIENTATION,OUTPUT_FORMAT,PANCHROMATIC_LINES,PANCHROMATIC_SAMPLES,L1_AVAILABLE,REFLECTIVE_LINES,REFLECTIVE_SAMPLES,RESAMPLING_OPTION,SCAN_GAP_INTERPOLATION,THERMAL_LINES,THERMAL_SAMPLES,TRUE_SCALE_LAT,UTM_ZONE,VERTICAL_LON_FROM_POLE,PRESENT_BAND_1,PRESENT_BAND_2,PRESENT_BAND_3,PRESENT_BAND_4,PRESENT_BAND_5,PRESENT_BAND_6,PRESENT_BAND_7,PRESENT_BAND_8,NADIR_OFFNADIR) FROM STDIN WITH 
		CSV 
		HEADER
	"""
cursor.copy_expert(sql=copy_statement, file=csvfile)

# drop unneeded columns to improve later query performance
drop_col_statement = """
	ALTER TABLE official_meta_full
		DROP COLUMN dateUpdated,
		DROP COLUMN browseAvailable,
		DROP COLUMN browseURL,
		DROP COLUMN sceneCenterLatitude,
		DROP COLUMN sceneCenterLongitude,
		DROP COLUMN cloudCoverFull,
		DROP COLUMN FULL_UL_QUAD_CCA,
		DROP COLUMN FULL_UR_QUAD_CCA,
		DROP COLUMN FULL_LL_QUAD_CCA,
		DROP COLUMN FULL_LR_QUAD_CCA,
		DROP COLUMN dayOrNight,
		DROP COLUMN flightPath,
		DROP COLUMN sunElevation,
		DROP COLUMN sunAzimuth,
		DROP COLUMN receivingStation,
		DROP COLUMN sceneStartTime,
		DROP COLUMN sceneStopTime,
		DROP COLUMN lookAngle,
		DROP COLUMN imageQuality1,
		DROP COLUMN imageQuality2,
		DROP COLUMN gainBand1,
		DROP COLUMN gainBand2,
		DROP COLUMN gainBand3,
		DROP COLUMN gainBand4,
		DROP COLUMN gainBand5,
		DROP COLUMN gainBand6H,
		DROP COLUMN gainBand6L,
		DROP COLUMN gainBand7,
		DROP COLUMN gainBand8,
		DROP COLUMN gainChangeBand1,
		DROP COLUMN gainChangeBand2,
		DROP COLUMN gainChangeBand3,
		DROP COLUMN gainChangeBand4,
		DROP COLUMN gainChangeBand5,
		DROP COLUMN gainChangeBand6H,
		DROP COLUMN gainChangeBand6L,
		DROP COLUMN gainChangeBand7,
		DROP COLUMN gainChangeBand8,
		DROP COLUMN satelliteNumber,
		DROP COLUMN cartURL,
		DROP COLUMN DATE_ACQUIRED_GAP_FILL,
		DROP COLUMN DATA_TYPE_L0RP,
		DROP COLUMN DATUM,
		DROP COLUMN ELEVATION_SOURCE,
		DROP COLUMN ELLIPSOID,
		DROP COLUMN EPHEMERIS_TYPE,
		DROP COLUMN FALSE_EASTING,
		DROP COLUMN FALSE_NORTHING,
		DROP COLUMN GAP_FILL,
		DROP COLUMN GROUND_CONTROL_POINTS_MODEL,
		DROP COLUMN GROUND_CONTROL_POINTS_VERIFY,
		DROP COLUMN GEOMETRIC_RMSE_MODEL,
		DROP COLUMN GEOMETRIC_RMSE_MODEL_X,
		DROP COLUMN GEOMETRIC_RMSE_MODEL_Y,
		DROP COLUMN GEOMETRIC_RMSE_VERIFY,
		DROP COLUMN GRID_CELL_SIZE_PANCHROMATIC,
		DROP COLUMN GRID_CELL_SIZE_REFLECTIVE,
		DROP COLUMN GRID_CELL_SIZE_THERMAL,
		DROP COLUMN MAP_PROJECTION_L1,
		DROP COLUMN MAP_PROJECTION_L0RA,
		DROP COLUMN ORIENTATION,
		DROP COLUMN OUTPUT_FORMAT,
		DROP COLUMN PANCHROMATIC_LINES,
		DROP COLUMN PANCHROMATIC_SAMPLES,
		DROP COLUMN L1_AVAILABLE,
		DROP COLUMN REFLECTIVE_LINES,
		DROP COLUMN REFLECTIVE_SAMPLES,
		DROP COLUMN RESAMPLING_OPTION,
		DROP COLUMN SCAN_GAP_INTERPOLATION,
		DROP COLUMN THERMAL_LINES,
		DROP COLUMN THERMAL_SAMPLES,
		DROP COLUMN TRUE_SCALE_LAT,
		DROP COLUMN UTM_ZONE,
		DROP COLUMN VERTICAL_LON_FROM_POLE,
		DROP COLUMN PRESENT_BAND_1,
		DROP COLUMN PRESENT_BAND_2,
		DROP COLUMN PRESENT_BAND_3,
		DROP COLUMN PRESENT_BAND_4,
		DROP COLUMN PRESENT_BAND_5,
		DROP COLUMN PRESENT_BAND_6,
		DROP COLUMN PRESENT_BAND_7,
		DROP COLUMN PRESENT_BAND_8,
		DROP COLUMN NADIR_OFFNADIR
	"""
cursor.execute(drop_col_statement)

# create geometry from coordinates
geom_statement = """
	UPDATE official_meta_full
	SET the_geom = ST_GeomFromText('POLYGON(('||upperleftcornerlongitude||' '||upperleftcornerlatitude||',
		'||upperrightcornerlongitude||' '||upperrightcornerlatitude||',
		'||lowerrightcornerlongitude||' '||lowerrightcornerlatitude||',
		'||lowerleftcornerlongitude||' '||lowerleftcornerlatitude||',
		'||upperleftcornerlongitude||' '||upperleftcornerlatitude||'
		))');
	"""
cursor.execute(geom_statement)

# commit transaction
conn.commit()






