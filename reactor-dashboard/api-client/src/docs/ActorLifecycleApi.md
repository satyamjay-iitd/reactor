# ActorLifecycleApi

All URIs are relative to *http://localhost*

|Method | HTTP request | Description|
|------------- | ------------- | -------------|
|[**actorAdded**](#actoradded) | **POST** /actor_added | |
|[**getStatus**](#getstatus) | **GET** /status | |
|[**startActor**](#startactor) | **POST** /start_actor | |
|[**stopActor**](#stopactor) | **POST** /stop_actor/{actor_addr} | |
|[**stopAllActors**](#stopallactors) | **POST** /stop_all_actors | |

# **actorAdded**
> actorAdded(remoteActorInfo)


### Example

```typescript
import {
    ActorLifecycleApi,
    Configuration,
    RemoteActorInfo
} from './api';

const configuration = new Configuration();
const apiInstance = new ActorLifecycleApi(configuration);

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

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**201** | Notify actor start on remote |  -  |
|**400** | &#x60;hostname&#x60; is not an IP address |  -  |
|**500** | The node controller is not running |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **getStatus**
> StatusResponse getStatus()


### Example

```typescript
import {
    ActorLifecycleApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new ActorLifecycleApi(configuration);

const { status, data } = await apiInstance.getStatus();
```

### Parameters
This endpoint does not have any parameters.


### Return type

**StatusResponse**

### Authorization

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | Status of the node |  -  |
|**500** | The node controller is not running |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **startActor**
> RemoteActorInfo startActor(spawnArgs)


### Example

```typescript
import {
    ActorLifecycleApi,
    Configuration,
    SpawnArgs
} from './api';

const configuration = new Configuration();
const apiInstance = new ActorLifecycleApi(configuration);

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

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**201** | Started; &#x60;hostname&#x60; is the node\&#39;s host as addressed by the client |  -  |
|**400** | The operator failed to start, e.g. because of an invalid payload |  -  |
|**404** | No such library or operator |  -  |
|**409** | An actor with this name already exists |  -  |
|**500** | The node controller is not running |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **stopActor**
> stopActor()


### Example

```typescript
import {
    ActorLifecycleApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new ActorLifecycleApi(configuration);

let actorAddr: string; //Address of the actor to stop (default to undefined)

const { status, data } = await apiInstance.stopActor(
    actorAddr
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **actorAddr** | [**string**] | Address of the actor to stop | defaults to undefined|


### Return type

void (empty response body)

### Authorization

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | Actor stop initiated |  -  |
|**404** | Actor not found |  -  |
|**500** | The node controller is not running |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **stopAllActors**
> stopAllActors()


### Example

```typescript
import {
    ActorLifecycleApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new ActorLifecycleApi(configuration);

const { status, data } = await apiInstance.stopAllActors();
```

### Parameters
This endpoint does not have any parameters.


### Return type

void (empty response body)

### Authorization

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | Actors stop initiated |  -  |
|**500** | The node controller is not running |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

