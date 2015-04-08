package com.datastax.driver.examples.rcp.ui;

import java.util.Collection;

import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.LabelProvider;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.ui.ISharedImages;
import org.eclipse.ui.PlatformUI;
import org.eclipse.ui.part.ViewPart;
import org.osgi.framework.ServiceReference;

import com.datastax.driver.examples.rcp.mailbox.MailboxActivator;
import com.datastax.driver.examples.rcp.mailbox.MailboxException;
import com.datastax.driver.examples.rcp.mailbox.MailboxMessage;
import com.datastax.driver.examples.rcp.mailbox.MailboxService;

public class View extends ViewPart {

    public static final String ID = "com.datastax.driver.examples.rcp.ui.View";

    private TableViewer viewer;

    private MailboxService service;

    /**
     * The content provider class is responsible for providing objects to the
     * view. It can wrap existing objects in adapters or simply return objects
     * as-is. These objects may be sensitive to the current input of the view,
     * or ignore it and always show the same content (like Task List, for
     * example).
     */
    class ViewContentProvider implements IStructuredContentProvider {

        @Override
        public void inputChanged(Viewer v, Object oldInput, Object newInput) {
        }

        @Override
        public void dispose() {
        }

        @Override
        public Object[] getElements(Object parent) {
            if (parent instanceof Object[]) {
                return (Object[]) parent;
            }
            return new Object[0];
        }
    }

    class ViewLabelProvider extends LabelProvider implements ITableLabelProvider {

        @Override
        public String getColumnText(Object obj, int index) {
            return getText(obj);
        }

        @Override
        public Image getColumnImage(Object obj, int index) {
            return getImage(obj);
        }

        @Override
        public Image getImage(Object obj) {
            return PlatformUI.getWorkbench().getSharedImages().getImage(ISharedImages.IMG_OBJ_ELEMENT);
        }
    }

    /**
     * This is a callback that will allow us to create the viewer and initialize
     * it.
     */
    @Override
    public void createPartControl(Composite parent) {
        // Mailbox host bundle should have been activated when we get here
        ServiceReference<?> reference = MailboxActivator.bundleContext.getServiceReference(MailboxService.class.getName());
        service = (MailboxService) MailboxActivator.bundleContext.getService(reference);
        viewer = new TableViewer(parent, SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL);
        viewer.setContentProvider(new ViewContentProvider());
        viewer.setLabelProvider(new ViewLabelProvider());
        // Provide the input to the ContentProvider
        try {
            Collection<MailboxMessage> messages = service.getMessages("user@datastax.com");
        } catch (MailboxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        viewer.setInput(new String[] { "One", "Two", "Three" });
    }

    /**
     * Passing the focus request to the viewer's control.
     */
    @Override
    public void setFocus() {
        viewer.getControl().setFocus();
    }
}