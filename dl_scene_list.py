import urllib
import gzip

# here comes the scheduler, but it involves also csv-->db so I'll do it later
# can be done with "schedule" package, already installed; see bookmarks

# download scene_list.gz from website
urllib.urlretrieve("http://landsat.usgs.gov/metadata_service/bulk_metadata_files/LANDSAT_8.csv.gz", "scene-list/LANDSAT_8.csv.gz")

# unpack the zipped file into a csv
zipped = gzip.open("scene-list/LANDSAT_8.csv.gz", "rb")
unzipped = open("scene-list/LANDSAT_8.csv", "wb")
unzipped.write(zipped.read())
zipped.close()
unzipped.close()



### CHANGE THE LINK TO DOWNLOAD OFFICIAL METADATA