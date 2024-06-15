import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

public class GenerateInput {
    public static void main(String[] args) {
        // Prompt the user to enter number of pairs
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter the number of pairs:");
        int numPairs = scanner.nextInt();
        System.out.println("Enter the radius:");
        int radius = scanner.nextInt(); // Read the radius from user input
        scanner.close();

        try {
            // Open a BufferedWriter to write to a file
            BufferedWriter writer = new BufferedWriter(new FileWriter("input.txt"));

            // Write the radius to the file
            writer.write("Radius: " + radius + "\n");

            // Generate and write the random pairs to the file and terminal
            for (int i = 0; i < numPairs; i++) {
                int x = (int) (Math.random() * radius);
                int y = (int) (Math.random() * radius);
                String pair = x + " " + y;
                writer.write(pair + "\n");
                System.out.println(pair);  // Print the pair to the terminal
            }

            // Close the writer
            writer.close();

            System.out.println("Pairs written to input.txt successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
