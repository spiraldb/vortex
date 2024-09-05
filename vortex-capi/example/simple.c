#include <stdio.h>
#include <stdlib.h>
#include "vortex.h"

int c_library_export() {
	printf("Creating a new dtype\n");
	VortexDType *dtype_f32 = vortex_dtype_f32(false);
	printf("dtype enum: %d", vortex_dtype_info(dtype_f32));
	vortex_dtype_destroy(dtype_f32);
}
