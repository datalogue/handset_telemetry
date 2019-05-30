# Handset ontology data

This demo creates an ontology in respect of data collected off of handset devices.

This ontology was designed for the purpose of enabling a data product to be generated from mobile handset data from different sources.

To create a data product, the data must be:

* tagged and labeled
* standardized between sources
* have identifiable information obfuscated

Note, there are two classes of identifiable information here:

*  Personally identifiable information ("*PII*") (that is, information such as an account number, MAC address, or even geographical coordinates). This information will in most cases be obfuscated.
* Sensitive corporate information ("*SCI*") (that is, information about carriers, device manufacturers, etc, that may be sensitive). This will likely be selectively obfuscated: that is, a company that purchases their data will be able to see information about their brand, and anonymized data about all other brands.

This notebook will:

1. create an ontology that captures the various classes of data that are relevant to this exercise (e.g. PII, SCI, account data, device data, network telemetry and device telemetry)
2. use [US based handset data](https://github.com/datalogue/handset_ontology_demo/blob/master/data/5K_us_tutela.csv) to train a neural network model to be able to identify data in each class of the ontology
3. create data pipelines that utilize the trained model to produce the aforementioned data product out of US and European handset data
4. pass this data off to a structured database for use in visualization and other analytics.

This was trained on [US based handset data](https://github.com/datalogue/handset_ontology_demo/blob/master/data/5K_us_tutela.csv), and will, in the future, be enriched from [several other public data stores](https://github.com/datalogue/demo-data), especially for the fields that aren't currently represented by the sample data.

## The ontology:

Declassified Wireless Carrier Data
This is for the purpose of cleaning and delivering safe data as a product.
```
Sensitive Data
   |___Subscriber
      |___Full Name
      |___First Name
      |___Family Name
      |___Account Identifier
      |___Address
         |___Subscriber Street
         |___Subscriber City
         |___Subscriber Zip
   |___Sensitive Device Data
      |___Sensitive Device Identifier
         |___Mac Address
         |___IMEI
      |___Sensitive Device Telemetry
         |___Sensitive Geospatial
            |___Latitude
            |___Longitude
            |___Altitude
            |___Geo Hash
         |___IP Address
         |___Mobile Network Code
   |___Commercially Sensitive Information
      |___Network Provider
      |___Manufacturer
      |___Device ID
      |___OS
      |___Device Model
Data to Distribute
   |___Device Data
      |___Screen Resolution
         |___Screen Resolution Width
         |___Screen Resolution Height
      |___Device Memory
      |___Device Storage
      |___Device Language
   |___Device Telemetry
      |___Device Hardware Performance
         |___Device System Uptime
         |___Device Used Storage
         |___Device Unused Storage
         |___Device Used Memory
         |___Device Unused Memory
         |___Device CPU Usage
         |___Device Battery Usage
         |___Device Battery State
      |___Device Geospatial Data
         |___Device Horizontal Accuracy
         |___Device Vertical Accuracy
         |___Device Velocity Speed
         |___Device Velocity Bearing
         |___Session State
         |___Session Country
   |___Network Telemetry
      |___Session Information
         |___Session Connection Type
         |___Session Connection Technology
         |___Session Time
            |___Session Start
            |___Session End
            |___Session Timezone
            |___QoS Timestamp
         |___Session Delta Transmitted Bytes
         |___Session Delta Receivede Bytes
      |___Mobile Network
         |___Mobile Connection Generation
         |___Mobile Channel
         |___Mobile Country Code
         |___Base Station Identity Code
         |___Physical Cell Identifier
         |___Reference Signal Received Power
         |___Reference Signal Received Quality
         |___Reference Signal Signal to Noise Ratio
         |___Channel Quality Indicator
         |___Timing Advance
      |___WiFi Network
         |___BSSID
         |___Wifi Frequency
      |___Quality of Service
         |___Upload Throughput
         |___Download Throughput
         |___Latency Average
         |___Link Speed
         |___Signal Strength
         |___Jitter
         |___Packet Loss
```

### Ontology customer feedback

> Looks pretty solid. Under the WiFi Network, perhaps SSID, Channel, MCS and Encryption would be useful.

## Model metrics:

[model metrics](https://github.com/datalogue/handset_telemetry/blob/master/code/images/Screen%20Shot%202019-05-30%20at%209.38.37%20AM.png?raw=true)
