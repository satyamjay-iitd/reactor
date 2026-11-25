# ChaosApi

All URIs are relative to *http://localhost*

|Method | HTTP request | Description|
|------------- | ------------- | -------------|
|[**setDuplication**](#setduplication) | **POST** /set_duplication | |
|[**setMsgDelay**](#setmsgdelay) | **POST** /set_msg_delay | |
|[**setMsgLoss**](#setmsgloss) | **POST** /set_msg_loss | |
|[**unsetMsgDelay**](#unsetmsgdelay) | **POST** /unset_msg_delay | |
|[**unsetMsgDuplication**](#unsetmsgduplication) | **POST** /unset_msg_duplication | |
|[**unsetMsgLoss**](#unsetmsgloss) | **POST** /unset_msg_loss | |

# **setDuplication**
> setDuplication(msgDuplicationRequest)


### Example

```typescript
import {
    ChaosApi,
    Configuration,
    MsgDuplicationRequest
} from './api';

const configuration = new Configuration();
const apiInstance = new ChaosApi(configuration);

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

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | Msg Duplication Config Applied |  -  |
|**404** | Actor not found |  -  |
|**500** | The node controller is not running |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **setMsgDelay**
> setMsgDelay(msgDelayRequest)


### Example

```typescript
import {
    ChaosApi,
    Configuration,
    MsgDelayRequest
} from './api';

const configuration = new Configuration();
const apiInstance = new ChaosApi(configuration);

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

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | Msg Delay Config Applied |  -  |
|**404** | Actor not found |  -  |
|**500** | The node controller is not running |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **setMsgLoss**
> setMsgLoss(msgLossRequest)


### Example

```typescript
import {
    ChaosApi,
    Configuration,
    MsgLossRequest
} from './api';

const configuration = new Configuration();
const apiInstance = new ChaosApi(configuration);

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

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | Msg Loss Config Applied |  -  |
|**404** | Actor not found |  -  |
|**500** | The node controller is not running |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **unsetMsgDelay**
> unsetMsgDelay(disableMsgDelayRequest)


### Example

```typescript
import {
    ChaosApi,
    Configuration,
    DisableMsgDelayRequest
} from './api';

const configuration = new Configuration();
const apiInstance = new ChaosApi(configuration);

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

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | Msg Delay Config Removed |  -  |
|**404** | Actor not found |  -  |
|**500** | The node controller is not running |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **unsetMsgDuplication**
> unsetMsgDuplication(body)


### Example

```typescript
import {
    ChaosApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new ChaosApi(configuration);

let body: string; //Name of the actor to stop duplicating messages, as a JSON string (or plain text)

const { status, data } = await apiInstance.unsetMsgDuplication(
    body
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **body** | **string**| Name of the actor to stop duplicating messages, as a JSON string (or plain text) | |


### Return type

void (empty response body)

### Authorization

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

 - **Content-Type**: text/plain
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | Msg Duplication Config Removed |  -  |
|**404** | Actor not found |  -  |
|**500** | The node controller is not running |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **unsetMsgLoss**
> unsetMsgLoss(body)


### Example

```typescript
import {
    ChaosApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new ChaosApi(configuration);

let body: string; //Name of the actor to stop losing messages, as a JSON string (or plain text)

const { status, data } = await apiInstance.unsetMsgLoss(
    body
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **body** | **string**| Name of the actor to stop losing messages, as a JSON string (or plain text) | |


### Return type

void (empty response body)

### Authorization

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

 - **Content-Type**: text/plain
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | Msg Loss Config Removed |  -  |
|**404** | Actor not found |  -  |
|**500** | The node controller is not running |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

