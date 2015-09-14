if (!String.prototype.trim) {
  (function() {
    // Make sure we trim BOM and NBSP
    var rtrim = /^[\s\uFEFF\xA0]+|[\s\uFEFF\xA0]+$/g;
    String.prototype.trim = function() {
        return this.replace(rtrim, '');
    };
  })();
}

(function(window) {
  function basePath() {
    var regexp = new RegExp('js/app.js');
    var script = $('script').filter(function(i, el) {
      return el.src.match(regexp);
    })[0]

    var base = script.src.substr(window.location.protocol.length + window.location.host.length + 2, script.src.length);

    return base.replace('/js/app.js', '');
  }

  var app = window.angular.module('docs', ['cfp.hotkeys'])

  app.value('pages', {"/":{"title":"Home","summary":"Home <small class=\"text-muted\">page</small>","path":"/","version":"2.2.0-rc3"},"/features/address_resolution/":{"title":"Address resolution","summary":"Address resolution <small class=\"text-muted\">page</small>","path":"/features/address_resolution/","version":"2.2.0-rc3"},"/features/compression/":{"title":"Compression","summary":"Compression <small class=\"text-muted\">page</small>","path":"/features/compression/","version":"2.2.0-rc3"},"/features/custom_codecs/":{"title":"Custom Codecs","summary":"Custom Codecs <small class=\"text-muted\">page</small>","path":"/features/custom_codecs/","version":"2.2.0-rc3"},"/features/custom_payloads/":{"title":"Custom Payloads","summary":"Custom Payloads <small class=\"text-muted\">page</small>","path":"/features/custom_payloads/","version":"2.2.0-rc3"},"/features/logging/":{"title":"Logging","summary":"Logging <small class=\"text-muted\">page</small>","path":"/features/logging/","version":"2.2.0-rc3"},"/features/metadata/":{"title":"Metadata","summary":"Metadata <small class=\"text-muted\">page</small>","path":"/features/metadata/","version":"2.2.0-rc3"},"/features/native_protocol/":{"title":"Native protocol","summary":"Native protocol <small class=\"text-muted\">page</small>","path":"/features/native_protocol/","version":"2.2.0-rc3"},"/features/object_mapper/creating/":{"title":"Definition of mapped classes","summary":"Definition of mapped classes <small class=\"text-muted\">page</small>","path":"/features/object_mapper/creating/","version":"2.2.0-rc3"},"/features/object_mapper/custom_codecs/":{"title":"Using custom codecs","summary":"Using custom codecs <small class=\"text-muted\">page</small>","path":"/features/object_mapper/custom_codecs/","version":"2.2.0-rc3"},"/features/object_mapper/":{"title":"Object Mapper","summary":"Object Mapper <small class=\"text-muted\">page</small>","path":"/features/object_mapper/","version":"2.2.0-rc3"},"/features/object_mapper/using/":{"title":"Using the mapper","summary":"Using the mapper <small class=\"text-muted\">page</small>","path":"/features/object_mapper/using/","version":"2.2.0-rc3"},"/features/paging/":{"title":"Paging","summary":"Paging <small class=\"text-muted\">page</small>","path":"/features/paging/","version":"2.2.0-rc3"},"/features/pooling/":{"title":"Connection pooling","summary":"Connection pooling <small class=\"text-muted\">page</small>","path":"/features/pooling/","version":"2.2.0-rc3"},"/features/query_timestamps/":{"title":"Query timestamps","summary":"Query timestamps <small class=\"text-muted\">page</small>","path":"/features/query_timestamps/","version":"2.2.0-rc3"},"/features/":{"title":"Features","summary":"Features <small class=\"text-muted\">page</small>","path":"/features/","version":"2.2.0-rc3"},"/features/shaded_jar/":{"title":"Using the shaded JAR","summary":"Using the shaded JAR <small class=\"text-muted\">page</small>","path":"/features/shaded_jar/","version":"2.2.0-rc3"},"/features/speculative_execution/":{"title":"Speculative query execution","summary":"Speculative query execution <small class=\"text-muted\">page</small>","path":"/features/speculative_execution/","version":"2.2.0-rc3"},"/changelog/":{"title":"Changelog","summary":"Changelog <small class=\"text-muted\">page</small>","path":"/changelog/","version":"2.2.0-rc3"},"/upgrade_guide/":{"title":"Upgrade guide","summary":"Upgrade guide <small class=\"text-muted\">page</small>","path":"/upgrade_guide/","version":"2.2.0-rc3"},"/faq/":{"title":"Frequently Asked Questions","summary":"Frequently Asked Questions <small class=\"text-muted\">page</small>","path":"/faq/","version":"2.2.0-rc3"},"/2.2.0-rc2/":{"title":"Home","summary":"Home <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/","version":"2.2.0-rc2"},"/2.2.0-rc2/features/address_resolution/":{"title":"Address resolution","summary":"Address resolution <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/features/address_resolution/","version":"2.2.0-rc2"},"/2.2.0-rc2/features/compression/":{"title":"Compression","summary":"Compression <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/features/compression/","version":"2.2.0-rc2"},"/2.2.0-rc2/features/custom_codecs/":{"title":"Custom Codecs","summary":"Custom Codecs <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/features/custom_codecs/","version":"2.2.0-rc2"},"/2.2.0-rc2/features/custom_payloads/":{"title":"Custom Payloads","summary":"Custom Payloads <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/features/custom_payloads/","version":"2.2.0-rc2"},"/2.2.0-rc2/features/logging/":{"title":"Logging","summary":"Logging <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/features/logging/","version":"2.2.0-rc2"},"/2.2.0-rc2/features/metadata/":{"title":"Metadata","summary":"Metadata <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/features/metadata/","version":"2.2.0-rc2"},"/2.2.0-rc2/features/native_protocol/":{"title":"Native protocol","summary":"Native protocol <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/features/native_protocol/","version":"2.2.0-rc2"},"/2.2.0-rc2/features/object_mapper/creating/":{"title":"Definition of mapped classes","summary":"Definition of mapped classes <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/features/object_mapper/creating/","version":"2.2.0-rc2"},"/2.2.0-rc2/features/object_mapper/custom_codecs/":{"title":"Using custom codecs","summary":"Using custom codecs <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/features/object_mapper/custom_codecs/","version":"2.2.0-rc2"},"/2.2.0-rc2/features/object_mapper/":{"title":"Object Mapper","summary":"Object Mapper <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/features/object_mapper/","version":"2.2.0-rc2"},"/2.2.0-rc2/features/object_mapper/using/":{"title":"Using the mapper","summary":"Using the mapper <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/features/object_mapper/using/","version":"2.2.0-rc2"},"/2.2.0-rc2/features/paging/":{"title":"Paging","summary":"Paging <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/features/paging/","version":"2.2.0-rc2"},"/2.2.0-rc2/features/pooling/":{"title":"Connection pooling","summary":"Connection pooling <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/features/pooling/","version":"2.2.0-rc2"},"/2.2.0-rc2/features/query_timestamps/":{"title":"Query timestamps","summary":"Query timestamps <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/features/query_timestamps/","version":"2.2.0-rc2"},"/2.2.0-rc2/features/":{"title":"Features","summary":"Features <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/features/","version":"2.2.0-rc2"},"/2.2.0-rc2/features/shaded_jar/":{"title":"Using the shaded JAR","summary":"Using the shaded JAR <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/features/shaded_jar/","version":"2.2.0-rc2"},"/2.2.0-rc2/features/speculative_execution/":{"title":"Speculative query execution","summary":"Speculative query execution <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/features/speculative_execution/","version":"2.2.0-rc2"},"/2.2.0-rc2/changelog/":{"title":"Changelog","summary":"Changelog <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/changelog/","version":"2.2.0-rc2"},"/2.2.0-rc2/upgrade_guide/":{"title":"Upgrade guide","summary":"Upgrade guide <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/upgrade_guide/","version":"2.2.0-rc2"},"/2.2.0-rc2/faq/":{"title":"Frequently Asked Questions","summary":"Frequently Asked Questions <small class=\"text-muted\">page</small>","path":"/2.2.0-rc2/faq/","version":"2.2.0-rc2"},"/2.2.0-rc1/":{"title":"Home","summary":"Home <small class=\"text-muted\">page</small>","path":"/2.2.0-rc1/","version":"2.2.0-rc1"},"/2.2.0-rc1/features/address_resolution/":{"title":"Address resolution","summary":"Address resolution <small class=\"text-muted\">page</small>","path":"/2.2.0-rc1/features/address_resolution/","version":"2.2.0-rc1"},"/2.2.0-rc1/features/custom_payloads/":{"title":"Custom Payloads","summary":"Custom Payloads <small class=\"text-muted\">page</small>","path":"/2.2.0-rc1/features/custom_payloads/","version":"2.2.0-rc1"},"/2.2.0-rc1/features/logging/":{"title":"Logging","summary":"Logging <small class=\"text-muted\">page</small>","path":"/2.2.0-rc1/features/logging/","version":"2.2.0-rc1"},"/2.2.0-rc1/features/metadata/":{"title":"Metadata","summary":"Metadata <small class=\"text-muted\">page</small>","path":"/2.2.0-rc1/features/metadata/","version":"2.2.0-rc1"},"/2.2.0-rc1/features/native_protocol/":{"title":"Native protocol","summary":"Native protocol <small class=\"text-muted\">page</small>","path":"/2.2.0-rc1/features/native_protocol/","version":"2.2.0-rc1"},"/2.2.0-rc1/features/paging/":{"title":"Paging","summary":"Paging <small class=\"text-muted\">page</small>","path":"/2.2.0-rc1/features/paging/","version":"2.2.0-rc1"},"/2.2.0-rc1/features/pooling/":{"title":"Connection pooling","summary":"Connection pooling <small class=\"text-muted\">page</small>","path":"/2.2.0-rc1/features/pooling/","version":"2.2.0-rc1"},"/2.2.0-rc1/features/query_timestamps/":{"title":"Query timestamps","summary":"Query timestamps <small class=\"text-muted\">page</small>","path":"/2.2.0-rc1/features/query_timestamps/","version":"2.2.0-rc1"},"/2.2.0-rc1/features/":{"title":"Features","summary":"Features <small class=\"text-muted\">page</small>","path":"/2.2.0-rc1/features/","version":"2.2.0-rc1"},"/2.2.0-rc1/features/shaded_jar/":{"title":"Using the shaded JAR","summary":"Using the shaded JAR <small class=\"text-muted\">page</small>","path":"/2.2.0-rc1/features/shaded_jar/","version":"2.2.0-rc1"},"/2.2.0-rc1/features/speculative_execution/":{"title":"Speculative query execution","summary":"Speculative query execution <small class=\"text-muted\">page</small>","path":"/2.2.0-rc1/features/speculative_execution/","version":"2.2.0-rc1"},"/2.2.0-rc1/changelog/":{"title":"Changelog","summary":"Changelog <small class=\"text-muted\">page</small>","path":"/2.2.0-rc1/changelog/","version":"2.2.0-rc1"},"/2.2.0-rc1/upgrade_guide/":{"title":"Upgrade guide","summary":"Upgrade guide <small class=\"text-muted\">page</small>","path":"/2.2.0-rc1/upgrade_guide/","version":"2.2.0-rc1"},"/2.2.0-rc1/faq/":{"title":"Frequently Asked Questions","summary":"Frequently Asked Questions <small class=\"text-muted\">page</small>","path":"/2.2.0-rc1/faq/","version":"2.2.0-rc1"},"/2.1.7/":{"title":"Home","summary":"Home <small class=\"text-muted\">page</small>","path":"/2.1.7/","version":"2.1.7"},"/2.1.7/features/address_resolution/":{"title":"Address resolution","summary":"Address resolution <small class=\"text-muted\">page</small>","path":"/2.1.7/features/address_resolution/","version":"2.1.7"},"/2.1.7/features/compression/":{"title":"Compression","summary":"Compression <small class=\"text-muted\">page</small>","path":"/2.1.7/features/compression/","version":"2.1.7"},"/2.1.7/features/logging/":{"title":"Logging","summary":"Logging <small class=\"text-muted\">page</small>","path":"/2.1.7/features/logging/","version":"2.1.7"},"/2.1.7/features/metadata/":{"title":"Metadata","summary":"Metadata <small class=\"text-muted\">page</small>","path":"/2.1.7/features/metadata/","version":"2.1.7"},"/2.1.7/features/native_protocol/":{"title":"Native protocol","summary":"Native protocol <small class=\"text-muted\">page</small>","path":"/2.1.7/features/native_protocol/","version":"2.1.7"},"/2.1.7/features/object_mapper/creating/":{"title":"Definition of mapped classes","summary":"Definition of mapped classes <small class=\"text-muted\">page</small>","path":"/2.1.7/features/object_mapper/creating/","version":"2.1.7"},"/2.1.7/features/object_mapper/":{"title":"Object Mapper","summary":"Object Mapper <small class=\"text-muted\">page</small>","path":"/2.1.7/features/object_mapper/","version":"2.1.7"},"/2.1.7/features/object_mapper/using/":{"title":"Using the mapper","summary":"Using the mapper <small class=\"text-muted\">page</small>","path":"/2.1.7/features/object_mapper/using/","version":"2.1.7"},"/2.1.7/features/paging/":{"title":"Paging","summary":"Paging <small class=\"text-muted\">page</small>","path":"/2.1.7/features/paging/","version":"2.1.7"},"/2.1.7/features/pooling/":{"title":"Connection pooling","summary":"Connection pooling <small class=\"text-muted\">page</small>","path":"/2.1.7/features/pooling/","version":"2.1.7"},"/2.1.7/features/query_timestamps/":{"title":"Query timestamps","summary":"Query timestamps <small class=\"text-muted\">page</small>","path":"/2.1.7/features/query_timestamps/","version":"2.1.7"},"/2.1.7/features/":{"title":"Features","summary":"Features <small class=\"text-muted\">page</small>","path":"/2.1.7/features/","version":"2.1.7"},"/2.1.7/features/shaded_jar/":{"title":"Using the shaded JAR","summary":"Using the shaded JAR <small class=\"text-muted\">page</small>","path":"/2.1.7/features/shaded_jar/","version":"2.1.7"},"/2.1.7/features/speculative_execution/":{"title":"Speculative query execution","summary":"Speculative query execution <small class=\"text-muted\">page</small>","path":"/2.1.7/features/speculative_execution/","version":"2.1.7"},"/2.1.7/changelog/":{"title":"Changelog","summary":"Changelog <small class=\"text-muted\">page</small>","path":"/2.1.7/changelog/","version":"2.1.7"},"/2.1.7/upgrade_guide/":{"title":"Upgrade guide","summary":"Upgrade guide <small class=\"text-muted\">page</small>","path":"/2.1.7/upgrade_guide/","version":"2.1.7"},"/2.1.7/faq/":{"title":"Frequently Asked Questions","summary":"Frequently Asked Questions <small class=\"text-muted\">page</small>","path":"/2.1.7/faq/","version":"2.1.7"},"/2.1.6/":{"title":"Home","summary":"Home <small class=\"text-muted\">page</small>","path":"/2.1.6/","version":"2.1.6"},"/2.1.6/features/address_resolution/":{"title":"Address resolution","summary":"Address resolution <small class=\"text-muted\">page</small>","path":"/2.1.6/features/address_resolution/","version":"2.1.6"},"/2.1.6/features/logging/":{"title":"Logging","summary":"Logging <small class=\"text-muted\">page</small>","path":"/2.1.6/features/logging/","version":"2.1.6"},"/2.1.6/features/metadata/":{"title":"Metadata","summary":"Metadata <small class=\"text-muted\">page</small>","path":"/2.1.6/features/metadata/","version":"2.1.6"},"/2.1.6/features/native_protocol/":{"title":"Native protocol","summary":"Native protocol <small class=\"text-muted\">page</small>","path":"/2.1.6/features/native_protocol/","version":"2.1.6"},"/2.1.6/features/paging/":{"title":"Paging","summary":"Paging <small class=\"text-muted\">page</small>","path":"/2.1.6/features/paging/","version":"2.1.6"},"/2.1.6/features/pooling/":{"title":"Connection pooling","summary":"Connection pooling <small class=\"text-muted\">page</small>","path":"/2.1.6/features/pooling/","version":"2.1.6"},"/2.1.6/features/query_timestamps/":{"title":"Query timestamps","summary":"Query timestamps <small class=\"text-muted\">page</small>","path":"/2.1.6/features/query_timestamps/","version":"2.1.6"},"/2.1.6/features/":{"title":"Features","summary":"Features <small class=\"text-muted\">page</small>","path":"/2.1.6/features/","version":"2.1.6"},"/2.1.6/features/shaded_jar/":{"title":"Using the shaded JAR","summary":"Using the shaded JAR <small class=\"text-muted\">page</small>","path":"/2.1.6/features/shaded_jar/","version":"2.1.6"},"/2.1.6/features/speculative_execution/":{"title":"Speculative query execution","summary":"Speculative query execution <small class=\"text-muted\">page</small>","path":"/2.1.6/features/speculative_execution/","version":"2.1.6"},"/2.1.5/":{"title":"Home","summary":"Home <small class=\"text-muted\">page</small>","path":"/2.1.5/","version":"2.1.5"},"/2.1.5/features/address_resolution/":{"title":"Address resolution","summary":"Address resolution <small class=\"text-muted\">page</small>","path":"/2.1.5/features/address_resolution/","version":"2.1.5"},"/2.1.5/features/heartbeat/":{"title":"Connection heartbeat","summary":"Connection heartbeat <small class=\"text-muted\">page</small>","path":"/2.1.5/features/heartbeat/","version":"2.1.5"},"/2.1.5/features/":{"title":"Features","summary":"Features <small class=\"text-muted\">page</small>","path":"/2.1.5/features/","version":"2.1.5"},"/2.1.5/features/shaded_jar/":{"title":"Using the shaded JAR","summary":"Using the shaded JAR <small class=\"text-muted\">page</small>","path":"/2.1.5/features/shaded_jar/","version":"2.1.5"},"/2.0.11/":{"title":"Home","summary":"Home <small class=\"text-muted\">page</small>","path":"/2.0.11/","version":"2.0.11"},"/2.0.11/features/address_resolution/":{"title":"Address resolution","summary":"Address resolution <small class=\"text-muted\">page</small>","path":"/2.0.11/features/address_resolution/","version":"2.0.11"},"/2.0.11/features/async/":{"title":"Asynchronous programming","summary":"Asynchronous programming <small class=\"text-muted\">page</small>","path":"/2.0.11/features/async/","version":"2.0.11"},"/2.0.11/features/compression/":{"title":"Compression","summary":"Compression <small class=\"text-muted\">page</small>","path":"/2.0.11/features/compression/","version":"2.0.11"},"/2.0.11/features/logging/":{"title":"Logging","summary":"Logging <small class=\"text-muted\">page</small>","path":"/2.0.11/features/logging/","version":"2.0.11"},"/2.0.11/features/metadata/":{"title":"Metadata","summary":"Metadata <small class=\"text-muted\">page</small>","path":"/2.0.11/features/metadata/","version":"2.0.11"},"/2.0.11/features/native_protocol/":{"title":"Native protocol","summary":"Native protocol <small class=\"text-muted\">page</small>","path":"/2.0.11/features/native_protocol/","version":"2.0.11"},"/2.0.11/features/paging/":{"title":"Paging","summary":"Paging <small class=\"text-muted\">page</small>","path":"/2.0.11/features/paging/","version":"2.0.11"},"/2.0.11/features/pooling/":{"title":"Connection pooling","summary":"Connection pooling <small class=\"text-muted\">page</small>","path":"/2.0.11/features/pooling/","version":"2.0.11"},"/2.0.11/features/":{"title":"Features","summary":"Features <small class=\"text-muted\">page</small>","path":"/2.0.11/features/","version":"2.0.11"},"/2.0.11/features/shaded_jar/":{"title":"Using the shaded JAR","summary":"Using the shaded JAR <small class=\"text-muted\">page</small>","path":"/2.0.11/features/shaded_jar/","version":"2.0.11"},"/2.0.11/features/speculative_execution/":{"title":"Speculative query execution","summary":"Speculative query execution <small class=\"text-muted\">page</small>","path":"/2.0.11/features/speculative_execution/","version":"2.0.11"},"/2.0.11/features/statements/prepared/":{"title":"Prepared statements","summary":"Prepared statements <small class=\"text-muted\">page</small>","path":"/2.0.11/features/statements/prepared/","version":"2.0.11"},"/2.0.11/features/statements/":{"title":"Statements","summary":"Statements <small class=\"text-muted\">page</small>","path":"/2.0.11/features/statements/","version":"2.0.11"},"/2.0.11/changelog/":{"title":"Changelog","summary":"Changelog <small class=\"text-muted\">page</small>","path":"/2.0.11/changelog/","version":"2.0.11"},"/2.0.11/upgrade_guide/":{"title":"Upgrade guide","summary":"Upgrade guide <small class=\"text-muted\">page</small>","path":"/2.0.11/upgrade_guide/","version":"2.0.11"},"/2.0.11/faq/":{"title":"Frequently Asked Questions","summary":"Frequently Asked Questions <small class=\"text-muted\">page</small>","path":"/2.0.11/faq/","version":"2.0.11"},"/2.0.10.1/":{"title":"Home","summary":"Home <small class=\"text-muted\">page</small>","path":"/2.0.10.1/","version":"2.0.10.1"},"/2.0.10.1/features/address_resolution/":{"title":"Address resolution","summary":"Address resolution <small class=\"text-muted\">page</small>","path":"/2.0.10.1/features/address_resolution/","version":"2.0.10.1"},"/2.0.10.1/features/logging/":{"title":"Logging","summary":"Logging <small class=\"text-muted\">page</small>","path":"/2.0.10.1/features/logging/","version":"2.0.10.1"},"/2.0.10.1/features/metadata/":{"title":"Metadata","summary":"Metadata <small class=\"text-muted\">page</small>","path":"/2.0.10.1/features/metadata/","version":"2.0.10.1"},"/2.0.10.1/features/native_protocol/":{"title":"Native protocol","summary":"Native protocol <small class=\"text-muted\">page</small>","path":"/2.0.10.1/features/native_protocol/","version":"2.0.10.1"},"/2.0.10.1/features/paging/":{"title":"Paging","summary":"Paging <small class=\"text-muted\">page</small>","path":"/2.0.10.1/features/paging/","version":"2.0.10.1"},"/2.0.10.1/features/pooling/":{"title":"Connection pooling","summary":"Connection pooling <small class=\"text-muted\">page</small>","path":"/2.0.10.1/features/pooling/","version":"2.0.10.1"},"/2.0.10.1/features/":{"title":"Features","summary":"Features <small class=\"text-muted\">page</small>","path":"/2.0.10.1/features/","version":"2.0.10.1"},"/2.0.10.1/features/shaded_jar/":{"title":"Using the shaded JAR","summary":"Using the shaded JAR <small class=\"text-muted\">page</small>","path":"/2.0.10.1/features/shaded_jar/","version":"2.0.10.1"},"/2.0.10.1/features/speculative_execution/":{"title":"Speculative query execution","summary":"Speculative query execution <small class=\"text-muted\">page</small>","path":"/2.0.10.1/features/speculative_execution/","version":"2.0.10.1"},"/2.0.10/":{"title":"Home","summary":"Home <small class=\"text-muted\">page</small>","path":"/2.0.10/","version":"2.0.10"},"/2.0.10/features/address_resolution/":{"title":"Address resolution","summary":"Address resolution <small class=\"text-muted\">page</small>","path":"/2.0.10/features/address_resolution/","version":"2.0.10"},"/2.0.10/features/logging/":{"title":"Logging","summary":"Logging <small class=\"text-muted\">page</small>","path":"/2.0.10/features/logging/","version":"2.0.10"},"/2.0.10/features/metadata/":{"title":"Metadata","summary":"Metadata <small class=\"text-muted\">page</small>","path":"/2.0.10/features/metadata/","version":"2.0.10"},"/2.0.10/features/paging/":{"title":"Paging","summary":"Paging <small class=\"text-muted\">page</small>","path":"/2.0.10/features/paging/","version":"2.0.10"},"/2.0.10/features/pooling/":{"title":"Connection pooling","summary":"Connection pooling <small class=\"text-muted\">page</small>","path":"/2.0.10/features/pooling/","version":"2.0.10"},"/2.0.10/features/":{"title":"Features","summary":"Features <small class=\"text-muted\">page</small>","path":"/2.0.10/features/","version":"2.0.10"},"/2.0.10/features/shaded_jar/":{"title":"Using the shaded JAR","summary":"Using the shaded JAR <small class=\"text-muted\">page</small>","path":"/2.0.10/features/shaded_jar/","version":"2.0.10"},"/2.0.10/features/speculative_execution/":{"title":"Speculative query execution","summary":"Speculative query execution <small class=\"text-muted\">page</small>","path":"/2.0.10/features/speculative_execution/","version":"2.0.10"}})
  app.factory('basePath', basePath)
  app.provider('search', function() {
    function localSearchFactory($http, $timeout, $q, $rootScope, basePath) {
      $rootScope.searchReady = false;

      var fetch = $http.get(basePath + '/json/search-index.json')
                       .then(function(response) {
                         var index = lunr.Index.load(response.data)
                         $rootScope.searchReady = true;
                         return index;
                       });

      // The actual service is a function that takes a query string and
      // returns a promise to the search results
      // (In this case we just resolve the promise immediately as it is not
      // inherently an async process)
      return function(q) {
        return fetch.then(function(index) {
          var results = []
          index.search(q).forEach(function(hit) {
            results.push(hit.ref);
          });
          return results;
        })
      };
    };
    localSearchFactory.$inject = ['$http', '$timeout', '$q', '$rootScope', 'basePath'];

    function webWorkerSearchFactory($q, $rootScope, basePath) {
      $rootScope.searchReady = false;

      var searchIndex = $q.defer();
      var results;
      var worker = new Worker(basePath + '/js/search-worker.js');

      // The worker will send us a message in two situations:
      // - when the index has been built, ready to run a query
      // - when it has completed a search query and the results are available
      worker.onmessage = function(e) {
        switch(e.data.e) {
          case 'ready':
            worker.postMessage({ e: 'load', p: basePath });
            break
          case 'index-ready':
            $rootScope.$apply(function() {
              $rootScope.searchReady = true;
            })
            searchIndex.resolve();
            break;
          case 'query-ready':
            results.resolve(e.data.d);
            break;
        }
      };

      // The actual service is a function that takes a query string and
      // returns a promise to the search results
      return function(q) {

        // We only run the query once the index is ready
        return searchIndex.promise.then(function() {

          results = $q.defer();
          worker.postMessage({ e: 'search', q: q });
          return results.promise;
        });
      };
    };
    webWorkerSearchFactory.$inject = ['$q', '$rootScope', 'basePath'];

    return {
      $get: window.Worker ? webWorkerSearchFactory : localSearchFactory
    };
  })

  app.controller('search', [
    '$scope',
    '$sce',
    '$timeout',
    'search',
    'pages',
    'basePath',
    function($scope, $sce, $timeout, search, pages, basePath) {
      $scope.hasResults = false;
      $scope.results = null;
      $scope.current = null;

      function clear() {
        $scope.hasResults = false;
        $scope.results = null;
        $scope.current = null;
      }

      $scope.search = function(version) {
        if ($scope.q.length >= 2) {
          search($scope.q).then(function(ids) {
            var results = []

            ids.forEach(function(id) {
              var page = pages[id];

              if (page.version == version) {
                results.push(page)
              }
            })

            if (results.length > 0) {
              $scope.hasResults = true;
              $scope.results = results;
              $scope.current = 0;
            } else {
              clear()
            }
          })
        } else {
          clear()
        }
      };

      $scope.basePath = basePath;

      $scope.reset = function() {
        $scope.q = null;
        clear()
      }

      $scope.submit = function() {
        var result = $scope.results[$scope.current]

        if (result) {
          $timeout(function() {
            window.location.href = basePath + result.path;
          })
        }
      }

      $scope.summary = function(item) {
        return $sce.trustAsHtml(item.summary);
      }

      $scope.moveDown = function(e) {
        if ($scope.hasResults && $scope.current < ($scope.results.length - 1)) {
          $scope.current++
          e.stopPropagation()
        }
      }

      $scope.moveUp = function(e) {
        if ($scope.hasResults && $scope.current > 0) {
          $scope.current--
          e.stopPropagation()
        }
      }
    }
  ])

  app.directive('search', [
    '$document',
    'hotkeys',
    function($document, hotkeys) {
      return function(scope, element, attrs) {
        hotkeys.add({
          combo: '/',
          description: 'Search docs...',
          callback: function(event, hotkey) {
            event.preventDefault()
            event.stopPropagation()
            element[0].focus()
          }
        })
      }
    }
  ])

  $(function() {
    $('#content').height(
      Math.max(
        $(".side-nav").height(),
        $('#content').height()
      )
    );

    $('#table-of-contents').on('activate.bs.scrollspy', function() {
      var active = $('#table-of-contents li.active').last().children('a');
      var button = $('#current-section');
      var text   = active.text().trim();

      if (active.length == 0 || text == 'Page Top') {
        button.html('Jump to... <span class="caret"></span><span class="sr-only">Table of Contents</span>')
      } else {
        if (text.length > 30) {
          text = text.slice(0, 30) + '...'
        }
        button.html('Viewing: ' + text + ' <span class="caret"></span><span class="sr-only">Table of Contents</span>')
      }
    })

    // Config ZeroClipboard
    ZeroClipboard.config({
      swfPath: basePath() + '/flash/ZeroClipboard.swf',
      hoverClass: 'btn-clipboard-hover',
      activeClass: 'btn-clipboard-active'
    })

    // Insert copy to clipboard button before .highlight
    $('.highlight').each(function () {
      var btnHtml = '<div class="zero-clipboard"><span class="btn-clipboard">Copy</span></div>'
      $(this).before(btnHtml)
    })

    var zeroClipboard = new ZeroClipboard($('.btn-clipboard'))

    // Handlers for ZeroClipboard

    // Copy to clipboard
    zeroClipboard.on('copy', function (event) {
      var clipboard = event.clipboardData;
      var highlight = $(event.target).parent().nextAll('.highlight').first()
      clipboard.setData('text/plain', highlight.text())
    })

    // Notify copy success and reset tooltip title
    zeroClipboard.on('aftercopy', function (event) {
      $(event.target)
        .attr('title', 'Copied!')
        .tooltip('fixTitle')
        .tooltip('show')
    })

    // Notify copy failure
    zeroClipboard.on('error', function (event) {
      $(event.target)
        .attr('title', 'Flash required')
        .tooltip('fixTitle')
        .tooltip('show')
    })
  })
})(window)
