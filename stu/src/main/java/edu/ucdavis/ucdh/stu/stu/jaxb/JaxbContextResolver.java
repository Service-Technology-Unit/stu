package edu.ucdavis.ucdh.stu.stu.jaxb;

import javax.ws.rs.Produces;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * JAXB Context Resolver
 */
@Provider
@Produces("text/xml")
public class JaxbContextResolver implements ContextResolver<Marshaller> {
	private final static String XML_HEADERS = "com.sun.xml.bind.xmlHeaders";
	private Log log = LogFactory.getLog(getClass());
	private String xslLocation = null;
	private JAXBContext context = null;

	/**
	 * Default constructor
	 * 
	 * @param xslLocation the location of the XSL stylesheet files
	 * @param annotatedPackages the annotatedPackages for this context
	 */
	public JaxbContextResolver(String xslLocation, String annotatedPackages) {
		this.xslLocation = xslLocation;
		if (!xslLocation.endsWith("/")) {
			this.xslLocation = xslLocation + "/";
		}
		log.info("Using XSL location \"" + this.xslLocation + "\".");
		try {
			this.context = JAXBContext.newInstance(annotatedPackages);
			log.info("Annotated packages: " + annotatedPackages);
		} catch (JAXBException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @param clazz the Class to be marshalled
	 */
	public Marshaller getContext(Class<?> clazz) {
		Marshaller marshaller = null;

		String styleSheetUri = xslLocation + clazz.getSimpleName().toLowerCase() + ".xsl";
		try {
			marshaller = context.createMarshaller();
			marshaller.setProperty(XML_HEADERS, "<?xml-stylesheet type=\"text/xsl\" href=\"" + styleSheetUri + "\"?>");
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
		} catch (JAXBException e) {
			log.error("Exception creating marshaller for class " + clazz.getName(), e);
		}

		return marshaller;
	}
}
