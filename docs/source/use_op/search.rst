Search
==================

Only vector
::
  curl -XPOST -H "content-type: application/json" -d'
  {
    "query":{
      "sum" : [
        {
           "field": "feature1",
           "feature": [0, 0, 0, 0, 0, 0],
        }
      ]
    }
  }
  ' http://xxxxxx/$db_name/$space_name/_search?size=10
            

Use threshold
::
  curl -XPOST -H "content-type: application/json" -d'
  { 
    "query":{
      "sum" : [
        {
           "field": "feature1",
           "feature": [0, 0, 0, 0, 0, 0],
           "min_score": 0.9
        }
      ]
    }
  }
  ' http://xxxxxx/$db_name/$space_name/_search?size=10


Use Numbrical filtration
::
  curl -XPOST -H "content-type: application/json" -d'
  {
    "query":{
      "sum" : [
        {
           "field": "feature1",
           "feature": [0, 0, 0, 0, 0, 0],
           "min_score": 0.9
        }
      ],
      "filter": [
        {
          "range": {
            "number_filed": {
               "gte" : 1,
               "lte" : 3
            }
          }
        }
      ]
    }
  }
  ' http://xxxxxx/$db_name/$space_name/_search?size=10

Use term filter
::
  curl -XPOST -H "content-type: application/json" -d'
  {
    "query":{
      "sum" : [
        {
           "field": "feature1",
           "feature": [0, 0, 0, 0, 0, 0],
           "min_score": 0.9
        }
      ],
      "filter": [
        {
          "range": {
            "number_filed": {
               "gte" : 1,
               "lte" : 3
            }
          },
          "term": {
            "tags":["t1","t2"],
            "operator":"and"
          }
        }
      ]
    }
  }
  ' http://xxxxxx/$db_name/$space_name/_search?size=10

