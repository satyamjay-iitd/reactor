# DefaultApi

All URIs are relative to *http://localhost*

|Method | HTTP request | Description|
|------------- | ------------- | -------------|
|[**actorAdded**](#actoradded) | **POST** /actor_added | |
|[**getStatus**](#getstatus) | **GET** /status | |
|[**registerLib**](#registerlib) | **POST** /register_lib | |
|[**startActor**](#startactor) | **POST** /start_actor | |
|[**stopAllActors**](#stopallactors) | **POST** /stop_all_actors | |

# **actorAdded**
> actorAdded(remoteActorInfo)


### Example

```typescript
import {
    DefaultApi,
    Configuration,
    RemoteActorInfo
} from './api';

const configuration = new Configuration();
const apiInstance = new DefaultApi(configuration);

let remoteActorInfo: RemoteActorInfo; //Remote Actor Detail

const { status, data } = await apiInstance.actorAdded(
    remoteActorInfo
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **remoteActorInfo** | **RemoteActorInfo**| Remote Actor Detail | |


### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**201** | Notify actor start on remote |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **getStatus**
> StatusResponse getStatus()


### Example

```typescript
import {
    DefaultApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new DefaultApi(configuration);

const { status, data } = await apiInstance.getStatus();
```

### Parameters
This endpoint does not have any parameters.


### Return type

**StatusResponse**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | Status of the node |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **registerLib**
> registerLib(registrationArgs)


### Example

```typescript
import {
    DefaultApi,
    Configuration,
    RegistrationArgs
} from './api';

const configuration = new Configuration();
const apiInstance = new DefaultApi(configuration);

let registrationArgs: RegistrationArgs; //Arguments to compile an operator

const { status, data } = await apiInstance.registerLib(
    registrationArgs
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **registrationArgs** | **RegistrationArgs**| Arguments to compile an operator | |


### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**201** | Registration successful |  -  |
|**400** | Registration Unsuccessful |  -  |
|**501** | Registration Not Supported on this node |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **startActor**
> RemoteActorInfo startActor(spawnArgs)


### Example

```typescript
import {
    DefaultApi,
    Configuration,
    SpawnArgs
} from './api';

const configuration = new Configuration();
const apiInstance = new DefaultApi(configuration);

let spawnArgs: SpawnArgs; //Actor arguments as arbitrary JSON

const { status, data } = await apiInstance.startActor(
    spawnArgs
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **spawnArgs** | **SpawnArgs**| Actor arguments as arbitrary JSON | |


### Return type

**RemoteActorInfo**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**201** | Start a new actor |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **stopAllActors**
> stopAllActors()


### Example

```typescript
import {
    DefaultApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new DefaultApi(configuration);

const { status, data } = await apiInstance.stopAllActors();
```

### Parameters
This endpoint does not have any parameters.


### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | Actors stop initiated |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

