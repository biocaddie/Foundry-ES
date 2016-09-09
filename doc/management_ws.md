Web Services for Management Interface
=====================================

## Manage Organizations

### Create an organization (POST)

```
http://tavi.neuinfo.org:8080/foundry/api/cinergi/organization?action=create
```
[SMR note-- Can we make this more web-like. Resource is 'organization'

```
PUT to 
http://tavi.neuinfo.org:8080/foundry/api/cinergi/organization
```
with JSON body
```JSON
{ "organization-name": "UCSD" }
```
Returns (JSON) the id of the created organization if successful
```JSON
    {
       "id": "54458b09e4b067e38c361a0d"
    }
```

### get an organization

```
http://tavi.neuinfo.org:8080/foundry/api/cinergi/organization/54458b09e4b067e38c361a0d
```
Returns the matching organization as a JSON object (or HTTP 404 if no match found)
```JSON
    {
       "name": "UCSD",
       "id": "54458b09e4b067e38c361a0d"
    }
```

### query organizations by name

```
http://tavi.neuinfo.org:8080/foundry/api/cinergi/organization/search?organization-name=UCSD
```
Returns the matching organization as a JSON object (or HTTP 404 if no match found)
```JSON
    {
       "name": "UCSD",
       "id": "54458b09e4b067e38c361a0d"
    }
```


### Delete an organization (POST)

[can't we use http DELETE?]

```
http://tavi.neuinfo.org:8080/foundry/api/cinergi/organization?action=delete
```

with JSON body
```JSON
{ "id": "54458b09e4b067e38c361a0d" }
```
Returns HTTP 200 if the deletion was successful

## Manage Users

### Create an user (POST)

```
http://tavi.neuinfo.org:8080/foundry/api/cinergi/user?action=create
```

with JSON body
```JSON
{"username": "bozyurt",
 "password": "pwd",
 "email": "iozyurt@ucsd.edu",
 "firstName": "Burak",
 "middleName": "",
 "lastName": "Ozyurt" }
```
Returns (JSON) the id of the created user if successful
```JSON
{"id": "544591d2e4b067e38c361a0e"}  
```

### get an user

```
http://tavi.neuinfo.org:8080/foundry/api/cinergi/user/544591d2e4b067e38c361a0e
```
Returns the matching user as a JSON object (or HTTP 404 if no match found)
```JSON
{
  "username": "bozyurt",
  "password": "",
  "email": "iozyurt@ucsd.edu",
  "firstName": "Burak",
  "middleName": "",
  "lastName": "Ozyurt"
}
```

### query users by username

```
http://tavi.neuinfo.org:8080/foundry/api/cinergi/user/search?username=bozyurt
```
Returns the matching user as a JSON object (or HTTP 404 if no match found)
```JSON
{
  "username": "bozyurt",
  "password": "",
  "email": "iozyurt@ucsd.edu",
  "firstName": "Burak",
  "middleName": "",
  "lastName": "Ozyurt"
}
```


### Delete an user (POST)

```
http://tavi.neuinfo.org:8080/foundry/api/cinergi/user?action=delete
```

with JSON body
```JSON
{"id": "544591d2e4b067e38c361a0e"}  
```
Returns HTTP 200 if the deletion was successful.
