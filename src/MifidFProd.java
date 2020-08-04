import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class MifidFProd {

	static final String MUREX = "MX";
	static final String NOMBREBASEFICH="FAM_MIFID_";

	/*
	 * Se lee el fichero linea a linea Si la linea empieza por MX Se obtiene el
	 * nombre del producto y su valoracion MIFID y si ok se añade a la lista de
	 * PL a ejecutar
	 * 
	 * 
	 * Se ejecutan las llamadas al PL de Carga MIFID
	 */
	
	
	/*Devuevle si el nombre de fichero dado cumple el formato del nombre de fichero valido*/
	private static boolean esNombreValido(String nf)
	{
		
		return nf.startsWith(NOMBREBASEFICH);
	}
	
	/*Dado un directorio busca el primer fichero que cumpla que tenga el formato en el nombre correcto de fichero y lo devuelve*/
	private static String obtenerFichero(String nd)
	{
		
		String res=null;
		File dir=new File(nd);
		boolean enc=false;
		
		String[] ficheros = dir.list();
		
		int i=0;
		while (i<ficheros.length && !enc)
		{
						
			if (esNombreValido(ficheros[i]))
			{
				
				res=ficheros[i];
				enc=true;
			}
			else i++;
		}
		return res;
	}
	
	public static void main(String[] args) {

		String nd="/tmp/os/MIFID/";
		String nf = "f1.txt";
		String nfaux=null;
		String nombrePL = "KYTL_GC.cargaMIFID";
		String error = "";
		String nfLog = "/tmp/os/MIFID/";

		int tam = args.length;

		// Si hay algun parametro
		if (tam != 0) {
			nd = args[0];
			System.out.println("El directorio de entrada es: " + nd);
			if (tam > 1) {
				nfLog = args[1];
				System.out.println("El fich de Log es: " + nfLog);

			}

		}

	
		nfaux=obtenerFichero(nd);
		
		if (nfaux!=null) 
			{
			
				nf=nd+""+nfaux;
				nfLog=nfLog+""+nfaux+".log";
			}
		
		
		File f = new File(nf);
		
			
		File fLog = new File(nfLog);

		if (!f.exists() || f.length() == 0) {
			error += "El fichero no existe o vacio\n";
		} else {
			
			DDBB db = new DDBB();

			db.ObtenerCredenciales();

			Connection c = db.ObtenerConexion();

			
			
			
			BufferedReader br = null;
			try {
				br = new BufferedReader(new FileReader(f));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				error += e.getMessage();
			}
			BufferedWriter bw = null;
			try {
				bw = new BufferedWriter(new FileWriter(nfLog));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				error += e1.getMessage();
			}

			ArrayList<String> l = new ArrayList<String>();

			String linea = null;
			int nlinea = 0;

			try {
				while ((linea = br.readLine()) != null) {
					nlinea++;

					if (linea.startsWith(MUREX)) {

						try {
							String p1 = linea.substring(8, 13);
							String p2 = linea.substring(13, 18);
							String p3 = linea.substring(18, 23);
							String producto = p1.trim() + ":" + p2.trim() + ":" + p3.trim() + ":";
							String v1 = linea.substring(23, 25);
							String v2 = linea.substring(25, 27);
							String valoracion = v1.trim() + v2.trim();
							// controlar vacia
							if (valoracion.length() == 0 || producto.length() == 0) {

								System.out.println("Linea numero: " + nlinea + " erronea: " + linea);
								bw.write("" + nlinea + ": erronea: " + linea);
								bw.newLine();

							} else {

								/*
								 * controlar que no exista el producto para el
								 * LOG
								 */

								// INI CONTROL LOG
								String sql = "select count (iscd_oid)  from ft_t_eist where DATA_SRC_ID='MUREX' and Data_Stat_typ='ACTIVE' and ext_iss_typ_txt= '"
										+ producto + "'";

								int contador = 0;
								try {
									contador = sqlCuenta(c, sql);

									// si no existe el producto al log
									if (contador == 0) {
										bw.write(nlinea+": No existe el producto:" + producto );
										bw.newLine();
									} else // si existe el prod
										if (contador > 1) {
										bw.write(nlinea+": El producto:" + producto + "  tiene mas de una entrada. ");
										bw.newLine();

									} else {
										String iscd_oid = null;
										// Si tiene mas de un mifid tb al log
										sql = "SELECT  iscd_oid  from ft_t_eist where  DATA_SRC_ID='MUREX' and Data_Stat_typ='ACTIVE' and ext_iss_typ_txt= '"
												+ producto + "'";
										iscd_oid = sqlString(c, sql);

										sql = "select count(EIST_OID)  from ft_t_eist where data_stat_typ='ACTIVE' and DATA_SRC_ID='MIFID' and ISCD_OID='"
												+ iscd_oid + "'";
										contador = sqlCuenta(c, sql);

										// tiene mas de una valoracion MIFID
										if (contador > 1) {
											bw.write(nlinea+": El producto:" + producto
													+ " tiene mas de una valoracion MIFID**.");
											bw.newLine();

										} else // Todo ok -> al PL
										{

											String params = "('" + producto + "','" + valoracion + "')";
											l.add(nombrePL + params);
										}

									}
								} catch (SQLException e) {

									error += e.getMessage();
								}

								// FIN CONTROL LOG

							}
						} catch (StringIndexOutOfBoundsException e) {
							error += e.getMessage() + " | LINEA: " + nlinea;
							bw.write(e.getMessage() + " |Falta algun dato| LINEA: " + nlinea);
							bw.newLine();

						}

					}

				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				error += e.getMessage();

			}

			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			System.out.println(l);

			try {
				realizarLlamadasPL(l, c);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				error += e.getMessage();
			}
			
			
				try {
			c.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		}

	

		if (error.length() > 0)
			System.out.println("[ERR]" + error);
		else
			System.out.println("OK");
	}

	/*Devuelve la busqueda de una sentencia sql de un String*/
	private static String sqlString(Connection c, String sql) throws SQLException {
		String iscd_oid;
		Statement stmnt2 = c.createStatement();
		ResultSet r2 = stmnt2.executeQuery(sql);
		r2.next();
		iscd_oid = r2.getString(1);
		return iscd_oid;
	}

	/*Devuelve la cuenta de una sentencia sql count*/
	private static int sqlCuenta(Connection c, String sql) throws SQLException {
		int contador;
		Statement stmnt2 = c.createStatement();
		ResultSet r2 = stmnt2.executeQuery(sql);
		r2.next();
		contador = r2.getInt(1);
		return contador;
	}

	/*Realiza las llamdadas a PL/SQL*/
	private static void realizarLlamadasPL(ArrayList<String> l, Connection c) throws SQLException {
		CallableStatement cs = null;

		int SIZE = l.size();

		for (int i = 0; i < SIZE; i++) {
			String llamada = l.get(i);
			cs = c.prepareCall("{call " + llamada + "}");

			cs.execute();
			cs.close();

		}
	}

}
