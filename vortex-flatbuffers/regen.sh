#!/usr/bin/env bash


# NOTE: order is important, the files must be provided in topological order

flatc \
	--rust \
	--filename-suffix "" \
	-I ./flatbuffers/ \
	-o ./src/generated \
	./flatbuffers/vortex-dtype/dtype.fbs \
	./flatbuffers/vortex-scalar/scalar.fbs \
	./flatbuffers/vortex-array/array.fbs

	#--include-prefix "flatbuffers::deps" \
# Modify the files as necessary to update the imports.
#sed -i '' ''
