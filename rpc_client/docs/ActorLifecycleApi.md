# \ActorLifecycleApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**actor_added**](ActorLifecycleApi.md#actor_added) | **POST** /actor_added | 
[**get_status**](ActorLifecycleApi.md#get_status) | **GET** /status | 
[**start_actor**](ActorLifecycleApi.md#start_actor) | **POST** /start_actor | 
[**stop_actor**](ActorLifecycleApi.md#stop_actor) | **POST** /stop_actor/{actor_addr} | 
[**stop_all_actors**](ActorLifecycleApi.md#stop_all_actors) | **POST** /stop_all_actors | 



## actor_added

> actor_added(remote_actor_info)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**remote_actor_info** | [**RemoteActorInfo**](RemoteActorInfo.md) | Remote Actor Detail | [required] |

### Return type

 (empty response body)

### Authorization

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_status

> models::StatusResponse get_status()


### Parameters

This endpoint does not need any parameter.

### Return type

[**models::StatusResponse**](StatusResponse.md)

### Authorization

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## start_actor

> models::RemoteActorInfo start_actor(spawn_args)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**spawn_args** | [**SpawnArgs**](SpawnArgs.md) | Actor arguments as arbitrary JSON | [required] |

### Return type

[**models::RemoteActorInfo**](RemoteActorInfo.md)

### Authorization

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## stop_actor

> stop_actor(actor_addr)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**actor_addr** | **String** | Address of the actor to stop | [required] |

### Return type

 (empty response body)

### Authorization

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## stop_all_actors

> stop_all_actors()


### Parameters

This endpoint does not need any parameter.

### Return type

 (empty response body)

### Authorization

[bearer_auth](../README.md#bearer_auth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

