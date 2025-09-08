# \DefaultApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**actor_added**](DefaultApi.md#actor_added) | **POST** /actor_added | 
[**get_status**](DefaultApi.md#get_status) | **GET** /status | 
[**register_lib**](DefaultApi.md#register_lib) | **POST** /register_lib | 
[**set_duplication**](DefaultApi.md#set_duplication) | **POST** /set_duplication | 
[**set_msg_delay**](DefaultApi.md#set_msg_delay) | **POST** /set_msg_delay | 
[**set_msg_loss**](DefaultApi.md#set_msg_loss) | **POST** /set_msg_loss | 
[**start_actor**](DefaultApi.md#start_actor) | **POST** /start_actor | 
[**stop_actor**](DefaultApi.md#stop_actor) | **POST** /stop_actor | 
[**stop_all_actors**](DefaultApi.md#stop_all_actors) | **POST** /stop_all_actors | 
[**unset_msg_delay**](DefaultApi.md#unset_msg_delay) | **POST** /unset_msg_delay | 
[**unset_msg_duplication**](DefaultApi.md#unset_msg_duplication) | **POST** /unset_msg_duplication | 
[**unset_msg_loss**](DefaultApi.md#unset_msg_loss) | **POST** /unset_msg_loss | 



## actor_added

> actor_added(remote_actor_info)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**remote_actor_info** | [**RemoteActorInfo**](RemoteActorInfo.md) | Remote Actor Detail | [required] |

### Return type

 (empty response body)

### Authorization

No authorization required

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

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## register_lib

> register_lib(registration_args)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**registration_args** | [**RegistrationArgs**](RegistrationArgs.md) | Arguments to compile an operator | [required] |

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## set_duplication

> set_duplication(msg_duplication_request)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**msg_duplication_request** | [**MsgDuplicationRequest**](MsgDuplicationRequest.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## set_msg_delay

> set_msg_delay(msg_delay_request)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**msg_delay_request** | [**MsgDelayRequest**](MsgDelayRequest.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## set_msg_loss

> set_msg_loss(msg_loss_request)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**msg_loss_request** | [**MsgLossRequest**](MsgLossRequest.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

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

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## stop_actor

> stop_actor(body)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**body** | **String** |  | [required] |

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: text/plain
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## stop_all_actors

> stop_all_actors()


### Parameters

This endpoint does not need any parameter.

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## unset_msg_delay

> unset_msg_delay(disable_msg_delay_request)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**disable_msg_delay_request** | [**DisableMsgDelayRequest**](DisableMsgDelayRequest.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## unset_msg_duplication

> unset_msg_duplication(body)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**body** | **String** |  | [required] |

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: text/plain
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## unset_msg_loss

> unset_msg_loss(body)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**body** | **String** |  | [required] |

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: text/plain
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

