#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <doca_aes_gcm.h>
#include <doca_comch_consumer.h>
#include <doca_dev.h>
#include <doca_dma.h>
#include <doca_dpa.h>
#include <doca_error.h>
#include <doca_pe.h>
#include <doca_sha.h>
#include <doca_version.h>

#include "buffer.h"
#include "common.h"
#include "comch_consumer.h"
#include "comch_msgq.h"
#include "comch_server.h"
#include "dma.h"
#include "object.h"
#include "dpa.h"

struct dmesh_doca_probe_report {
	uint32_t device_count;
	int32_t devinfo_status;
	int32_t open_status;
	int32_t close_status;
	int32_t dma_status;
	int32_t aes_gcm_status;
	int32_t sha_status;
	int32_t dpa_status;
	char compile_version[32];
	char runtime_version[32];
	char first_device_pci[64];
};

struct dmesh_doca_comch {
	struct objects objs;
};

static void cleanup_comch(struct dmesh_doca_comch *handle)
{
	if (handle == NULL)
		return;

	if (handle->objs.consumer != NULL)
		(void)doca_ctx_stop(doca_comch_consumer_as_ctx(handle->objs.consumer));

	while (handle->objs.consumer != NULL && handle->objs.consumer_pe != NULL) {
		enum doca_ctx_states state;
		doca_error_t result;

		(void)doca_pe_progress(handle->objs.consumer_pe);
		result = doca_ctx_get_state(doca_comch_consumer_as_ctx(handle->objs.consumer), &state);
		if (result != DOCA_SUCCESS || state == DOCA_CTX_STATE_IDLE)
			break;
	}

	clean_comch_consumer(handle->objs.consumer, handle->objs.consumer_pe);
	handle->objs.consumer = NULL;
	handle->objs.consumer_pe = NULL;

	if (handle->objs.consumer_mem != NULL) {
		clean_local_mem_bufs(handle->objs.consumer_mem);
		free(handle->objs.consumer_mem);
		handle->objs.consumer_mem = NULL;
	}

	cleanup_objects(&handle->objs);
}

static void copy_str(char *dst, size_t dst_len, const char *src)
{
	if (dst == NULL || dst_len == 0)
		return;

	if (src == NULL)
		src = "";

	snprintf(dst, dst_len, "%s", src);
}

static void init_report(struct dmesh_doca_probe_report *report)
{
	memset(report, 0, sizeof(*report));
	report->devinfo_status = DOCA_ERROR_UNEXPECTED;
	report->open_status = DOCA_ERROR_NOT_FOUND;
	report->close_status = DOCA_SUCCESS;
	report->dma_status = DOCA_ERROR_NOT_FOUND;
	report->aes_gcm_status = DOCA_ERROR_NOT_FOUND;
	report->sha_status = DOCA_ERROR_NOT_FOUND;
	report->dpa_status = DOCA_ERROR_NOT_FOUND;
	copy_str(report->compile_version, sizeof(report->compile_version), doca_version());
	copy_str(report->runtime_version, sizeof(report->runtime_version), doca_version_runtime());
}

int32_t dmesh_doca_probe(struct dmesh_doca_probe_report *report)
{
	struct doca_devinfo **dev_list = NULL;
	struct doca_dev *dev = NULL;
	struct doca_dma *dma = NULL;
	struct doca_aes_gcm *aes_gcm = NULL;
	struct doca_sha *sha = NULL;
	struct doca_dpa *dpa = NULL;
	uint32_t nb_devs = 0;
	doca_error_t result;

	if (report == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	init_report(report);

	result = doca_devinfo_create_list(&dev_list, &nb_devs);
	report->devinfo_status = result;
	report->device_count = nb_devs;
	if (result != DOCA_SUCCESS)
		return result;

	if (nb_devs == 0) {
		doca_devinfo_destroy_list(dev_list);
		return DOCA_ERROR_NOT_FOUND;
	}

	result = doca_devinfo_get_pci_addr_str(dev_list[0], report->first_device_pci);
	if (result != DOCA_SUCCESS)
		copy_str(report->first_device_pci, sizeof(report->first_device_pci), "<unknown>");

	result = doca_dev_open(dev_list[0], &dev);
	report->open_status = result;
	if (result != DOCA_SUCCESS) {
		doca_devinfo_destroy_list(dev_list);
		return result;
	}

	report->dma_status = doca_dma_create(dev, &dma);
	if (report->dma_status == DOCA_SUCCESS)
		doca_dma_destroy(dma);

	report->aes_gcm_status = doca_aes_gcm_create(dev, &aes_gcm);
	if (report->aes_gcm_status == DOCA_SUCCESS)
		doca_aes_gcm_destroy(aes_gcm);

	report->sha_status = doca_sha_create(dev, &sha);
	if (report->sha_status == DOCA_SUCCESS)
		doca_sha_destroy(sha);

	report->dpa_status = doca_dpa_create(dev, &dpa);
	if (report->dpa_status == DOCA_SUCCESS)
		doca_dpa_destroy(dpa);

	report->close_status = doca_dev_close(dev);
	doca_devinfo_destroy_list(dev_list);

	return DOCA_SUCCESS;
}

const char *dmesh_doca_error_name(int32_t error)
{
	return doca_error_get_name((doca_error_t)error);
}

const char *dmesh_doca_error_descr(int32_t error)
{
	return doca_error_get_descr((doca_error_t)error);
}

int32_t dmesh_doca_init(const char *dev_pci_addr,
						  const char *rep_pci_addr,
						  const char *server_name,
						  struct dmesh_doca_comch **handle)
{
	struct dmesh_doca_comch *comch;
	doca_error_t result;

	fprintf(stderr, "[DMesh] Initializing DOCA objects with dev_pci_addr=%s, rep_pci_addr=%s, server_name=%s\n",
		dev_pci_addr, rep_pci_addr, server_name ? server_name : "<default>");

	if (dev_pci_addr == NULL || rep_pci_addr == NULL || handle == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	if (server_name == NULL)
		server_name = "DPUMesh";

	*handle = NULL;

	comch = calloc(1, sizeof(*comch));
	if (comch == NULL)
		return DOCA_ERROR_NO_MEMORY;

	result = open_doca_device_with_pci(dev_pci_addr, NULL, &comch->objs.dev);
	if (result != DOCA_SUCCESS)
		goto free_handle;
	fprintf(stderr, "Opened DOCA device %s\n", dev_pci_addr);

	result = open_doca_device_rep_with_pci(comch->objs.dev,
					       DOCA_DEVINFO_REP_FILTER_NET,
					       rep_pci_addr,
					       &comch->objs.rep_dev);
	if (result != DOCA_SUCCESS)
		goto cleanup_handle;
	fprintf(stderr, "Opened DOCA device rep %s\n", rep_pci_addr);

	result = init_comch_ctrl_path_server(server_name, &comch->objs, true);
	if (result != DOCA_SUCCESS)
		goto cleanup_handle;
	fprintf(stderr, "Initialized DOCA comch server %s\n", server_name);

	result = init_comch_datapath_consumer(&comch->objs);
	if (result != DOCA_SUCCESS)
		goto cleanup_handle;
	fprintf(stderr, "Initialized DOCA comch datapath consumer\n");

	result = init_dpa_objects(&comch->objs);
	if (result != DOCA_SUCCESS)
		goto cleanup_handle;
	fprintf(stderr, "Initialized DOCA DPA objects\n");

	result = dmesh_doca_dpa_thread_create(comch->objs.dpa_thread);
	if (result != DOCA_SUCCESS)
		goto cleanup_handle;
	fprintf(stderr, "Initialized DOCA DPA thread\n");
	
	result = init_comch_dpa_msgq(&comch->objs, comch->objs.consumer_pe);
	if (result != DOCA_SUCCESS)
		goto cleanup_handle;
	fprintf(stderr, "Initialized DOCA DPA msgq\n");
	
	while (comch->objs.ring_mmap == NULL) {
		doca_pe_progress(comch->objs.pe);
	}
	fprintf(stderr, "Received ring mmap from host\n");

	/* setup DPA buffer array with remote mmap */
    result = setup_dpa_buf_array(&comch->objs, 1024, comch->objs.ring_mmap);
    if (result != DOCA_SUCCESS)
		goto cleanup_handle;
	fprintf(stderr, "Setup DPA buf array");

    /* allocate local buffer and set mmap for PCI export */
    result = alloc_buffer_and_set_mmap(&comch->objs.local_mmap, comch->objs.dev,
                           &comch->objs.dma_buffer, 1024*1024,
                           DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS)
		goto cleanup_handle;

	fprintf(stderr, "Waiting for remote mmap from host to be ready...\n");
    while (comch->objs.remote_mmap == NULL) {
		doca_pe_progress(comch->objs.pe);
    }
	fprintf(stderr, "Received remote mmap from host\n");	
	
    /* run DPA thread */
    result = dmesh_doca_run_dpa_thread(&comch->objs, comch->objs.dpa_thread, comch->objs.dpa_comch);
	if (result != DOCA_SUCCESS)
		goto cleanup_handle;
	fprintf(stderr, "Run DPA thread\n");	
	
    result = send_dma_request_to_dpa(&comch->objs);
    if (result != DOCA_SUCCESS)
		goto cleanup_handle;
	fprintf(stderr, "Sending DMA requests to DPA\n");	

	*handle = comch;
	return DOCA_SUCCESS;

cleanup_handle:
	cleanup_comch(comch);
free_handle:
	free(comch);
	return result;
}

int32_t dmesh_doca_comch_server_consumer_create(const char *dev_pci_addr,
						const char *rep_pci_addr,
						const char *server_name,
						struct dmesh_doca_comch **handle)
{
	return dmesh_doca_init(dev_pci_addr, rep_pci_addr, server_name, handle);
}

void dmesh_doca_comch_destroy(struct dmesh_doca_comch *handle)
{
	if (handle == NULL)
		return;

	cleanup_comch(handle);
	free(handle);
}
