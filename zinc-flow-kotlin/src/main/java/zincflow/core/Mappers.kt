package zincflow.core

import com.fasterxml.jackson.core.StreamReadFeature
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

object JsonMapper {
    @JvmStatic
    val mapper = jacksonObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
}

object YamlMapper {
    @JvmStatic
    val mapper = ObjectMapper(
        YAMLFactory.builder().configure(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION, true).build()
    ).findAndRegisterModules()
}
