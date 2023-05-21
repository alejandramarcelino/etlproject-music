# ETL Pipeline using data on concerts and music popularity

An ETL pipeline to determine whether concerts have an influence in music popularity trends, including current top songs and artists. 

## More information on the process

The data on upcoming concerts is “scraped” from concertful.com with the aid of the BeautifulSoup library, in addition to asyncio and aiohttp, in order to obtain the vast amount of data asynchronously. There are many ways to gauge music popularity. We look at the top50 USA songs playlist on Spotify and we gather the data using the Spotipy python library to easily access the Spotify Web API. This workflow helps us gather this data over time to ultimately analyze and answer any relevant questions. The project is still in progress.

Some general motivating questions 
- Do certain venues have a higher impact on music trends for artists playing genre X?
- Are there certain venues that regularly have an audience more open to new upcoming artists, or whether people gather primarily for already-big-artist-names? 
- Is there connection between state and genres having higher impact on song popularity?

## Installing dependencies

The project involves several python scripts, each having some dependencies. The scripts are orchestrated using airflow. It’s currently being run in my local environment, but you can choose to use services like Docker or AWS EC2 to allow the dag to run even when you are not connected to a network. Refer to the documentation on each to get more information and be sure to first look over AWS pricing if you’d like to use an EC2 instance. Most scripts use AWS S3 to upload and grab files— the free tier has been sufficient for the purpose of setting it up but most likely will need more space if data is collected over time.

* The list of the needed libraries and dependencies are included in the requirements.txt document

I recommend first creating a virtual environment. I used python 3.10.11 to create the virtual environment which you can install using Homebrew but depends on your system. Please reach out if you need assistance with downloading a previous version if you already have the updated one.

After creating and activating the virtual environment, you can use the command below to install the packages contained in the requirement.txt file:
	$ pip install -r requirements.txt
I used ‘pip3.10’ instead of ‘pip’ to install packages and store the contents in the python3.10 site-packages location within the virtual environment.

## Credentials
There are different options to include the needed credentials in the scripts. 

You can use a separate file with the sensitive information. I included a ‘pipeline_template.conf’ file, which you can make a copy of and save as ‘pipeline.conf’ to put your own information and avoid modifying the original file. 

An alternative is creating environment variables or setting up local configuration files/setting up configurations (using AWS CLI for example). To use environment variables:
	- If using macos or linux, can use:
		export <variable_name> = ‘<value_of_variable>’
	- If using windows, can use:
		set <variable_name> = ‘<value_of_variable>’
Then to reference the variable in the script, you import the os module and reference the variable name:

import os
os.environ[‘<variable_name>’]

* I use and reference the file so make the necessary adjustments to the code if you choose to use the environment variables.

Some additional things needed:
- The URL string to connect to your database when creating engine using SQLAlchemy (assuming you have a database created already with PostgreSQL)
- AWS account, an S3 bucket, an IAM user with the appropriate permissions (e.g. AmazonS3FullAccess)
	-Note: If you decide to use an EC2 instance, will need IAM user to give access permission (e.g.AmazonEC2FullAccess) and need the secret key/access key information to connect to the instance, via AWS CLI. Refer to documentation to get more information on different options/methods for using this service in general.

## Ways to contribute

Most of the scripts are running and functional for the intended purpose, which is mentioned in the comments of each document. This includes all scripts except those related to constructing and updating the database tables and relations. 

The ‘revised_spotify.py’ file includes the portion dealing with creating the list for the artists and i still need to work out the specific, since I wrote that before creating the junction tables in the database.

The ‘data_transfer.py’ script needs the most work, since I started generally writing out the intentions but not all the specifics are included. I referenced another script with functions involving database operations and that script may be altered as well to improve readability in the data_transfer document.

I also need to connect the data gathered on concerts/festivals to the data gathered from the top playlists. I am thinking of using the search function in the Spotipy library to search the performers from concerts on Spotify to gather data such as ids, genres, etc, to be able to bring the data together.

It would be great if I can collect the information retroactively to be able to analyze data since this will only be possible after collecting data for some time.

I also need to change the dag file after completing the changes to the database files. The paths to each file have to be manually added.

## Contact

Feel free to reach out to me through GitHub for any questions on the set up and or any other questions related to the project, as well as any feedback you would like to provide. You can open an issue or submit a pull request and we can talk about the project further.

** I may edit this document to include more details on running the pipeline and on any updates.

