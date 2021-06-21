## Problem Statement:

Suppose we want to show a map to visualize the annual net generation of power plants of the US.
The challenge consists of the following requirements:
· We want to display the top N plants.
· On the map we want to show absolute value and percentage of the plant's federal state.
· We want to be able to filter by state so we can zoom in.
· The data usually comes as excel file - https://www.epa.gov/energy/emissions-generation-resourceintegrated-
database-egrid (eGRID2016 Data File)
· Built JUST a python backend that backs this map with a REST API.
· Bonus: deployment of the solution in a cloud service.



## Plant State level Power for Map Service

Current implementation is a basic approch to solve problem statment. This will provide one API to fetch top N power plant on the basis of number of generators they have. It also give option to add filter on the basis of state. Response of this API can be presented over map.
Following technologies has been used to develop it.

 - Python 3.8(flask, pandas, pandasql)
 - Docker
 - Docker Compose

## Assumptions:
1. We are considering top plants on basis of number of generators they have
2. Federal State percentage for plant is calculated on basis of number of generators for plant out of total generators for that state

### Run Applications

```bash
-- Using Docker Compose:
Requirements:
 1. need to install Docker and Docker-compose and run
 2. docker-compose up
```
```bash
-- Using IDE/Command Line
Requirements:
 1. need to install python3.8, pip
 2. pip3 install -r requirements.txt
 3. python3.8 server.py
```

### Test APIs
```bash
//Fetch Plants

Fetch top 10 plants:
curl -X GET http://0.0.0.0:9080/topN?size=10

Fetch top 10 plants with filter on state
curl -X GET http://0.0.0.0:9080/topN?size=10&state=CA

Sample Response:
HTTP Code: 200
[
  {
    "actual_value": "9942",
    "aggregate_value_per_state": 197323836.96400014,
    "percentage": 0.0050384181419570835,
    "plant_lat": 38.2494,
    "plant_long": -122.0394,
    "plant_name": "Solano County Cogen Plant",
    "state": "CA"
  },
  {
    "actual_value": "9911",
    "aggregate_value_per_state": 197323836.96400014,
    "percentage": 0.005022707926467175,
    "plant_lat": 33.9716,
    "plant_long": -118.0481,
    "plant_name": "Whittier LFG Power Plant #1",
    "state": "CA"
  },...
  ]
```

### Features which could have been done:
- Need to add proper error handling
- Need to add proper logs mechanism
- Need to add the unit and integration testings
- Need to add automatic checks for enforcing code quality
- Need to add  authorization like JWT/OAuth to APIs
- Need to add CI/CD plugin for automatic deployment through docker
