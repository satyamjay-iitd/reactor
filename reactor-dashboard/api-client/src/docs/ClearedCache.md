# ClearedCache


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**freed_bytes** | **number** | Disk space the build directories took. | [default to undefined]
**unloaded** | **Array&lt;string&gt;** | The compiled libraries that were unloaded; the next request for one compiles it again. | [default to undefined]

## Example

```typescript
import { ClearedCache } from './api';

const instance: ClearedCache = {
    freed_bytes,
    unloaded,
};
```

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
