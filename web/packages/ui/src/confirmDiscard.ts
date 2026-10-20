import { ElMessageBox } from 'element-plus'

export interface DiscardLabels {
  title: string
  message: string
  confirm: string
  cancel: string
}

export async function confirmDiscardChanges(labels: DiscardLabels): Promise<boolean> {
  try {
    await ElMessageBox.confirm(labels.message, labels.title, {
      type: 'warning',
      closeOnClickModal: false,
      closeOnPressEscape: false,
      confirmButtonText: labels.confirm,
      cancelButtonText: labels.cancel,
    })
    return true
  } catch {
    return false
  }
}
