#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <doca_aes_gcm.h>
#include <doca_dev.h>
#include <doca_dma.h>
#include <doca_dpa.h>
#include <doca_error.h>
#include <doca_sha.h>
#include <doca_version.h>

struct linkerd_doca_probe_report {
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

static void copy_str(char *dst, size_t dst_len, const char *src)
{
	if (dst == NULL || dst_len == 0)
		return;

	if (src == NULL)
		src = "";

	snprintf(dst, dst_len, "%s", src);
}

static void init_report(struct linkerd_doca_probe_report *report)
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

int32_t linkerd_doca_probe(struct linkerd_doca_probe_report *report)
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

const char *linkerd_doca_error_name(int32_t error)
{
	return doca_error_get_name((doca_error_t)error);
}

const char *linkerd_doca_error_descr(int32_t error)
{
	return doca_error_get_descr((doca_error_t)error);
}
