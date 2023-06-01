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
** The list of the needed libraries and dependencies are included in the requirements.txt document **

I recommend first creating a virtual environment. I used python 3.10.11 to create the virtual environment which you can install using Homebrew but depends on your system. Please reach out if you need assistance with downloading a previous version if you already have the updated one.

After creating and activating the virtual environment, you can use the command below to install the packages contained in the requirement.txt file:
	$ pip install -r requirements.txt
I used ‘pip3.10’ instead of ‘pip’ to install packages and store the contents in the python3.10 site-packages location within the virtual environment.

## Airflow

I did not include the airflow configuration files to exclude any personal information. However, in order to run the DAG, you want to make some revisions to your 'airflow.cfg' file. You want to include your database connection string, which is automatically set to a mysql one when you first install airflow, but I recommend setting this to your postgresql database (under [database]). Also, the executor under [core] should be changed from SequentialExecutor to LocalExecutor. I also recommend setting parallelism to the number of CPU cores on your machine.

Please consult apach airflow documentation for more details.

Note: Before running the DAG file, you must manually put in the absolute paths of the files for each task.

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
All of the scripts are running and functional, including the database files. The database files include creating the tables and relations, and inserting new data each day while abiding by the defined integrity constraints.

In some files I include possible edits I might make in the future to improve the workflow and signficance of the data. I encourage users to fork this repository and open pull requests for any of these ideas or for any other suggestions you may have.

Some of the bigger (but reasonably doable) things left to do:
- To connect the data gathered on concerts/festivals to the data gathered from the top playlists, beyond genres and artists. 
- For the concerts table, the group of performers (assuming opener/opening act, headliner, etc) are all together under 'performer' and these should be broken up and linked to individual artists in the artist table. I did the process for the playlist/tracks, just need to apply the same idea to the concerts data.
- For the festivals, it would be better to get more specific information on the individual concerts within each festival. It may require using an alternate source. Overall this table can be broken down a lot more to be able to link it with the data on popular music/songs.


A slightly less doable edit: to collect the information retroactively to be able to analyze the data since this will only be possible after collecting data for some time.


## Contact

Feel free to reach out to me through GitHub for any questions on the set up and or any other questions related to the project, as well as any feedback you would like to provide. You can open an issue or submit a pull request and we can talk about the project further.

** I may edit this document to include more details on running the pipeline and on any updates.

