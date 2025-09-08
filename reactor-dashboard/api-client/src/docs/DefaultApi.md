# DefaultApi

All URIs are relative to *http://localhost*

|Method | HTTP request | Description|
|------------- | ------------- | -------------|
|[**actorAdded**](#actoradded) | **POST** /actor_added | |
|[**getStatus**](#getstatus) | **GET** /status | |
|[**registerLib**](#registerlib) | **POST** /register_lib | |
|[**setDuplication**](#setduplication) | **POST** /set_duplication | |
|[**setMsgDelay**](#setmsgdelay) | **POST** /set_msg_delay | |
|[**setMsgLoss**](#setmsgloss) | **POST** /set_msg_loss | |
|[**startActor**](#startactor) | **POST** /start_actor | |
|[**stopActor**](#stopactor) | **POST** /stop_actor | |
|[**stopAllActors**](#stopallactors) | **POST** /stop_all_actors | |
|[**unsetMsgDelay**](#unsetmsgdelay) | **POST** /unset_msg_delay | |
|[**unsetMsgDuplication**](#unsetmsgduplication) | **POST** /unset_msg_duplication | |
|[**unsetMsgLoss**](#unsetmsgloss) | **POST** /unset_msg_loss | |

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

# **setDuplication**
> setDuplication(msgDuplicationRequest)


### Example

```typescript
import {
    DefaultApi,
    Configuration,
    MsgDuplicationRequest
} from './api';

const configuration = new Configuration();
const apiInstance = new DefaultApi(configuration);

let msgDuplicationRequest: MsgDuplicationRequest; //

const { status, data } = await apiInstance.setDuplication(
    msgDuplicationRequest
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **msgDuplicationRequest** | **MsgDuplicationRequest**|  | |


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
|**200** | Msg Duplication Config Applied |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **setMsgDelay**
> setMsgDelay(msgDelayRequest)


### Example

```typescript
import {
    DefaultApi,
    Configuration,
    MsgDelayRequest
} from './api';

const configuration = new Configuration();
const apiInstance = new DefaultApi(configuration);

let msgDelayRequest: MsgDelayRequest; //

const { status, data } = await apiInstance.setMsgDelay(
    msgDelayRequest
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **msgDelayRequest** | **MsgDelayRequest**|  | |


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
|**200** | Msg Delay Config Applied |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **setMsgLoss**
> setMsgLoss(msgLossRequest)


### Example

```typescript
import {
    DefaultApi,
    Configuration,
    MsgLossRequest
} from './api';

const configuration = new Configuration();
const apiInstance = new DefaultApi(configuration);

let msgLossRequest: MsgLossRequest; //

const { status, data } = await apiInstance.setMsgLoss(
    msgLossRequest
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **msgLossRequest** | **MsgLossRequest**|  | |


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
|**200** | Msg Loss Config Applied |  -  |

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

# **stopActor**
> stopActor(body)


### Example

```typescript
import {
    DefaultApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new DefaultApi(configuration);

let body: string; //

const { status, data } = await apiInstance.stopActor(
    body
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **body** | **string**|  | |


### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: text/plain
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | Actor stop initiated |  -  |
|**404** | Actor not found |  -  |

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

# **unsetMsgDelay**
> unsetMsgDelay(disableMsgDelayRequest)


### Example

```typescript
import {
    DefaultApi,
    Configuration,
    DisableMsgDelayRequest
} from './api';

const configuration = new Configuration();
const apiInstance = new DefaultApi(configuration);

let disableMsgDelayRequest: DisableMsgDelayRequest; //

const { status, data } = await apiInstance.unsetMsgDelay(
    disableMsgDelayRequest
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **disableMsgDelayRequest** | **DisableMsgDelayRequest**|  | |


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
|**200** | Msg Delay Config Removed |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **unsetMsgDuplication**
> unsetMsgDuplication(body)


### Example

```typescript
import {
    DefaultApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new DefaultApi(configuration);

let body: string; //

const { status, data } = await apiInstance.unsetMsgDuplication(
    body
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **body** | **string**|  | |


### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: text/plain
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | Msg Duplication Config Removed |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **unsetMsgLoss**
> unsetMsgLoss(body)


### Example

```typescript
import {
    DefaultApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new DefaultApi(configuration);

let body: string; //

const { status, data } = await apiInstance.unsetMsgLoss(
    body
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **body** | **string**|  | |


### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: text/plain
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | Msg Loss Config Removed |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

