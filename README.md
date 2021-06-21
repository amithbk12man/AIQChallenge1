## Power Plant Map Service

This is very basic version of power plant map service. This will provide one API to fetch top N power plant on the basis of number of generators they have. It also give option to add filter on the basis of state. Response of this API can be presented over map.
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
curl -X GET http://0.0.0.0:8080/topN?size=10

Fetch top 10 plants with filter on state
curl -X GET http://0.0.0.0:8080/topN?size=10&state=AK

Sample Response:
HTTP Code: 200
[
  {
    "actual_value": 9,
    "percentage": 1.1703511053315996,
    "plant_lat": 64.67141,
    "plant_long": -147.075988,
    "plant_name": "Eielson AFB Central Heat & Power Plant",
    "state": "AK"
  },
  {
    "actual_value": 9,
    "percentage": 1.1703511053315996,
    "plant_lat": 62.110415,
    "plant_long": -145.532529,
    "plant_name": "Glennallen",
    "state": "AK"
  }
 ]
```

### Enhancement:
- Need to add proper error handling
- Need to add proper logs mechanism
- Need to add the unit and integration testings
- Need to add automatic checks for enforcing code quality
- Need to add  authorization like JWT/OAuth to APIs
- Need to add CI/CD plugin for automatic deployment through docker
