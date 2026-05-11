package zincflow.core

import com.fasterxml.jackson.core.StreamReadFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

object JsonMapper {
    @JvmStatic
    val mapper = jacksonObjectMapper()
}

object YamlMapper {
    @JvmStatic
    val mapper = ObjectMapper(
        YAMLFactory.builder().configure(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION, true).build()
    ).findAndRegisterModules()
}
