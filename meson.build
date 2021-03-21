project('kafka-cpp', 'cpp')

cpp = meson.get_compiler('cpp')

src_common = [
  'src/kafka.cc',
]

src_producer = [
  'src/producer.cc',
] + src_common

src_consumer = [
  'src/consumer.cc',
] + src_common

lib = [
  #dependency('lrdkafka'),
  cpp.find_library('librdkafka'),
  #cpp.find_library('librdkafka++'),
]

cpp_arg = [
    '-O3',
    '-march=native',
	'-g',
]


executable(
    'producer',
    sources: src_producer,
    dependencies : lib,
    cpp_args : cpp_arg,
)

executable(
    'consumer',
    sources: src_consumer,
    dependencies : lib,
    cpp_args : cpp_arg,
)
