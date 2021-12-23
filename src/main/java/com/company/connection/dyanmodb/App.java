package com.company.connection.dyanmodb;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
    	DynamodbConnection db=new DynamodbConnection();
        db.createTable("Music3", "Artist");
    	db.putItemInTable("Music3", "Artist", "Famous band", "AlbumTitle", "Songs About Life", "Awards", "10", "SongTitle", " Happy Day");
    	db.getDynamoDBItem("Music3", "Artist", "Famous band");
    }
}
