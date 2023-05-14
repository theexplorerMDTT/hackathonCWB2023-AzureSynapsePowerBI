# hackathonCWB2023-AzureSynapsePowerBI
This is for the hackathon Code Without Barrier Microsoft 2023, using Azure Synapse and Power BI

Step 1: Use the [>_] button to the right of the search bar at the top of the page to create a new Cloud Shell in the Azure portal, selecting a PowerShell environment and creating storage if prompted. 
In the PowerShell pane, enter the following commands to clone this repository:
rm -r hackathon2023CWB -f
git clone https://github.com/theexplorerMDTT/hackathonCWB2023-AzureSynapsePowerBI.git hackathon2023CWB

Then,
cd Implementation/
 ./setup.ps1
 
Run the setup.ps1 inside the Implementation folder.
This is to setup the Azure environment, including Azure resources, and synapse workspace
When set up, password will be required.
Please take note this password for Power BI linking later.

Step 2: 
Once the resource has been created.
Go to Azure Synapse > Open Azure Synapse Studio > Ingest Data > 
Create HTTP Service Connection as Source
And Destination: Azure Blob Storage
Goal: To get Zip files from the given dataset link

Step 3:
Use Spark Transform notebook to unzip the file and save it in Azure Blob Storage

Step 4
Download the mdb files that have been unzipped to local machine, 
Convert to CSV
Upload to Azure Blob Storage

Then, create Azure Synapse Dedicated SQL Pool
Then create Copy data activity to copy data from Azure Blob Storage to Azure Synapse Dedicated SQL Pool.
There are 4 csv tables, so we need to create 4 separate Copy Data Activity with different source and destination dataset.
This is for Power BI.

Step 5:

Then link Power BI with Azure Synapse.
Server" Using Dedicated SQL server
Username : SQLUser
Password: (Use the one created at the beginning)

Then create reports using the tables which locate in Azure Synapse SQL Pool
