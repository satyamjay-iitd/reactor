# CompileApi

All URIs are relative to *http://localhost*

|Method | HTTP request | Description|
|------------- | ------------- | -------------|
|[**buildLib**](#buildlib) | **POST** /builds | |
|[**cancelBuild**](#cancelbuild) | **DELETE** /builds/{lib_name} | |
|[**cancelBuilds**](#cancelbuilds) | **DELETE** /builds | |
|[**clearBuildCache**](#clearbuildcache) | **DELETE** /cache | |

# **buildLib**
> buildLib(compilationArgs)


### Example

```typescript
import {
    CompileApi,
    Configuration,
    CompilationArgs
} from './api';

const configuration = new Configuration();
const apiInstance = new CompileApi(configuration);

let compilationArgs: CompilationArgs; //Arguments to compile an operator

const { status, data } = await apiInstance.buildLib(
    compilationArgs
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **compilationArgs** | **CompilationArgs**| Arguments to compile an operator | |


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
|**200** | Already loaded, compiled from identical arguments; nothing to do |  -  |
|**201** | Compiled and loaded |  -  |
|**400** | Code generation or compilation failed (message in the body) |  -  |
|**409** | A library with this name is loaded (built from other arguments) or being built, or the build was cancelled |  -  |
|**500** | The node controller is not running |  -  |
|**501** | Compilation Not Supported on this node |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **cancelBuild**
> CancelledBuilds cancelBuild()


### Example

```typescript
import {
    CompileApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new CompileApi(configuration);

let libName: string; //The library being built (default to undefined)

const { status, data } = await apiInstance.cancelBuild(
    libName
);
```

### Parameters

|Name | Type | Description  | Notes|
|------------- | ------------- | ------------- | -------------|
| **libName** | [**string**] | The library being built | defaults to undefined|


### Return type

**CancelledBuilds**

### Authorization

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | Cancelled the library\&#39;s build (killing the compiler) |  -  |
|**404** | The library is not being built |  -  |
|**500** | The node controller is not running |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **cancelBuilds**
> CancelledBuilds cancelBuilds()


### Example

```typescript
import {
    CompileApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new CompileApi(configuration);

const { status, data } = await apiInstance.cancelBuilds();
```

### Parameters
This endpoint does not have any parameters.


### Return type

**CancelledBuilds**

### Authorization

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | Cancelled the builds in progress (killing the compiler) |  -  |
|**500** | The node controller is not running |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **clearBuildCache**
> ClearedCache clearBuildCache()


### Example

```typescript
import {
    CompileApi,
    Configuration
} from './api';

const configuration = new Configuration();
const apiInstance = new CompileApi(configuration);

const { status, data } = await apiInstance.clearBuildCache();
```

### Parameters
This endpoint does not have any parameters.


### Return type

**ClearedCache**

### Authorization

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
|**200** | Unloaded the compiled libraries and deleted their build directories. Their memory is released when the node restarts |  -  |
|**409** | Actors are running on this node, or libraries are being built |  -  |
|**500** | The directories could not be deleted, or the node controller is not running |  -  |
|**501** | Compilation Not Supported on this node |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

