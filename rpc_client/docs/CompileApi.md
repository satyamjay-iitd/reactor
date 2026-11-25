# \CompileApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**build_lib**](CompileApi.md#build_lib) | **POST** /builds | 
[**cancel_build**](CompileApi.md#cancel_build) | **DELETE** /builds/{lib_name} | 
[**cancel_builds**](CompileApi.md#cancel_builds) | **DELETE** /builds | 
[**clear_build_cache**](CompileApi.md#clear_build_cache) | **DELETE** /cache | 



## build_lib

> build_lib(compilation_args)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**compilation_args** | [**CompilationArgs**](CompilationArgs.md) | Arguments to compile an operator | [required] |

### Return type

 (empty response body)

### Authorization

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## cancel_build

> models::CancelledBuilds cancel_build(lib_name)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**lib_name** | **String** | The library being built | [required] |

### Return type

[**models::CancelledBuilds**](CancelledBuilds.md)

### Authorization

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## cancel_builds

> models::CancelledBuilds cancel_builds()


### Parameters

This endpoint does not need any parameter.

### Return type

[**models::CancelledBuilds**](CancelledBuilds.md)

### Authorization

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## clear_build_cache

> models::ClearedCache clear_build_cache()


### Parameters

This endpoint does not need any parameter.

### Return type

[**models::ClearedCache**](ClearedCache.md)

### Authorization

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

