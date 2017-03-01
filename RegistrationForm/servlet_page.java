import java.io.IOException;
import java.io.PrintWriter;
import java.io.*;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/Indexservelet")
public class Indexservelet extends HttpServlet {
	private static final long serialVersionUID = 1L;
    
  
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		//response.getWriter().append("Served at: ").append(request.getContextPath());
		
				response.setContentType("text/html");
				PrintWriter out=response.getWriter();
				
				
				//GET PARAMETERS
				
				String Age = request.getParameter("Age");
				String EduQual = request.getParameter("EduQual");
				String MaritalStatus = request.getParameter("MaritalStatus");
				String Gender =request.getParameter("Gender");
				String TaxStatus = request.getParameter("TaxStatus");
				String INCOME = request.getParameter("INCOME");
				String ParentsStatus = request.getParameter("ParentsStatus");
				String CobCountry = request.getParameter("CobCountry");
				String CitzCountry = request.getParameter("CitzCountry");
				String WkW = request.getParameter("WkW");
					
				System.out.println("welcome to the customer servlet ");
				
					
			
			
				//	Output on the Page Itself
				
				out.println("<html>");
				out.println("<body>");
				out.println("<br>");
				out.println(Age);
				out.println("<br>");
				out.println(EduQual);
				out.println("<br>");
				out.println(MaritalStatus);
				out.println("<br>");
				out.println(Gender);
				out.println("<br>");
				out.println(TaxStatus);
				out.println("<br>");
				out.println(INCOME);
				out.println("<br>");
				out.println(ParentsStatus);
				out.println("<br>");
				out.println(CobCountry);
				out.println("<br>");
				out.println(CitzCountry);
				out.println("<br>");
				out.println(WkW);
				out.println("<br>");
				out.println("</body>");
				out.println("</html>");
				
						
		///*******************************************************************************
				
					
					
				//ANALYSIS
				//Writing into the file 
					
			  
			    	
					File file=new File("C:/Users/MRuser/Downloads/Sample.txt");
					if(!file.exists())
					{
						file.createNewFile();
					}
					
				    FileWriter f1=new FileWriter ("C:/Users/MRuser/Downloads/Sample.txt", true);		
					BufferedWriter bw= new BufferedWriter(f1);
					
					bw.write(Age);
					bw.write(",");
					bw.write(EduQual);
					bw.write(",");
					bw.write(MaritalStatus);
					bw.write(",");	 
					bw.write(Gender);
					bw.write(",");
					bw.write(TaxStatus);
					bw.write(",");
					bw.write(INCOME);
					bw.write(",");
					bw.write(ParentsStatus);
					bw.write(",");
					bw.write(CobCountry);
					bw.write(",");
					bw.write(CitzCountry);
					bw.write(",");
					bw.write(WkW);
					
					bw.newLine();
					bw.close();
					
					
				
					
			    			    	
			                 

	}

}
