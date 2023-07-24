package com.lel.bigdatatermproject;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Menu extends javax.swing.JFrame {

    private Runner runner;

    /**
     * Creates new form Menu
     */
    public Menu() {
        runner = new Runner();
        initComponents();
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        task1 = new javax.swing.JButton();
        task2 = new javax.swing.JButton();
        task3 = new javax.swing.JButton();
        task4 = new javax.swing.JButton();
        task5 = new javax.swing.JButton();
        taskInputField = new java.awt.TextField();
        taskInputLabel = new java.awt.Label();
        taskOutputField = new java.awt.TextArea();
        runTasksHeader = new java.awt.Label();
        label1 = new java.awt.Label();
        label2 = new java.awt.Label();
        label3 = new java.awt.Label();
        localFilePathField = new java.awt.TextField();
        hdfsPathField = new java.awt.TextField();
        fileUploadButton = new javax.swing.JButton();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

        task1.setText("Total Number of Trips Per Taxi");
        task1.setHorizontalTextPosition(javax.swing.SwingConstants.LEFT);
        task1.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                task1MouseClicked(evt);
            }
        });

        task2.setText("Average Trip Distance Per Taxi");
        task2.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                task2MouseClicked(evt);
            }
        });

        task3.setText("Maximum Tip Amount");
        task3.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                task3MouseClicked(evt);
            }
        });

        task4.setText("Average Trip Distance Per Passanger Count");
        task4.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                task4MouseClicked(evt);
            }
        });

        task5.setText("Top 10 Busiest Pickup Locations");
        task5.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                task5MouseClicked(evt);
            }
        });

        taskInputField.setText("/input/");

        taskInputLabel.setAlignment(java.awt.Label.CENTER);
        taskInputLabel.setText("Input File Location");

        runTasksHeader.setAlignment(java.awt.Label.CENTER);
        runTasksHeader.setFont(new java.awt.Font("Consolas", 1, 24)); // NOI18N
        runTasksHeader.setText("Run Tasks");

        label1.setAlignment(java.awt.Label.CENTER);
        label1.setFont(new java.awt.Font("Consolas", 1, 24)); // NOI18N
        label1.setText("Upload Files to HDFS");

        label2.setAlignment(java.awt.Label.CENTER);
        label2.setFont(new java.awt.Font("Consolas", 1, 14)); // NOI18N
        label2.setText("Target HDFS Directory");

        label3.setAlignment(java.awt.Label.CENTER);
        label3.setFont(new java.awt.Font("Consolas", 1, 14)); // NOI18N
        label3.setText("File's Local Path");

        localFilePathField.setFont(new java.awt.Font("Consolas", 2, 10)); // NOI18N

        hdfsPathField.setCursor(new java.awt.Cursor(java.awt.Cursor.TEXT_CURSOR));
        hdfsPathField.setFont(new java.awt.Font("Consolas", 2, 10)); // NOI18N
        hdfsPathField.setText("/input");

        fileUploadButton.setText("Upload");
        fileUploadButton.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                fileUploadButtonMouseClicked(evt);
            }
        });

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(layout.createSequentialGroup()
                        .addGap(38, 38, 38)
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                            .addComponent(task5, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(task4, javax.swing.GroupLayout.DEFAULT_SIZE, 263, Short.MAX_VALUE)
                            .addComponent(task1, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(task2, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(task3, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                                .addComponent(taskInputLabel, javax.swing.GroupLayout.PREFERRED_SIZE, 115, javax.swing.GroupLayout.PREFERRED_SIZE)
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                .addComponent(taskInputField, javax.swing.GroupLayout.PREFERRED_SIZE, 130, javax.swing.GroupLayout.PREFERRED_SIZE)))
                        .addGap(44, 44, 44)
                        .addComponent(taskOutputField, javax.swing.GroupLayout.PREFERRED_SIZE, 458, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(layout.createSequentialGroup()
                        .addGap(331, 331, 331)
                        .addComponent(runTasksHeader, javax.swing.GroupLayout.PREFERRED_SIZE, 144, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(layout.createSequentialGroup()
                        .addGap(223, 223, 223)
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                            .addComponent(label2, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(label3, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                        .addGap(62, 62, 62)
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                            .addComponent(localFilePathField, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(hdfsPathField, javax.swing.GroupLayout.DEFAULT_SIZE, 169, Short.MAX_VALUE))))
                .addContainerGap(41, Short.MAX_VALUE))
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                .addGap(0, 0, Short.MAX_VALUE)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                        .addComponent(label1, javax.swing.GroupLayout.PREFERRED_SIZE, 278, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGap(275, 275, 275))
                    .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                        .addComponent(fileUploadButton, javax.swing.GroupLayout.PREFERRED_SIZE, 168, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGap(328, 328, 328))))
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(runTasksHeader, javax.swing.GroupLayout.PREFERRED_SIZE, 40, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(24, 24, 24)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING, false)
                    .addComponent(taskOutputField, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(task1, javax.swing.GroupLayout.PREFERRED_SIZE, 39, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGap(18, 18, 18)
                        .addComponent(task2, javax.swing.GroupLayout.PREFERRED_SIZE, 40, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGap(18, 18, 18)
                        .addComponent(task3, javax.swing.GroupLayout.PREFERRED_SIZE, 39, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGap(18, 18, 18)
                        .addComponent(task4, javax.swing.GroupLayout.PREFERRED_SIZE, 39, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGap(18, 18, 18)
                        .addComponent(task5, javax.swing.GroupLayout.PREFERRED_SIZE, 38, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGap(25, 25, 25)
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                            .addComponent(taskInputField, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(taskInputLabel, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 74, Short.MAX_VALUE)
                .addComponent(label1, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(38, 38, 38)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(localFilePathField, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addComponent(label3, javax.swing.GroupLayout.DEFAULT_SIZE, 42, Short.MAX_VALUE))
                .addGap(25, 25, 25)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(label2, javax.swing.GroupLayout.DEFAULT_SIZE, 42, Short.MAX_VALUE)
                    .addComponent(hdfsPathField, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                .addGap(34, 34, 34)
                .addComponent(fileUploadButton, javax.swing.GroupLayout.PREFERRED_SIZE, 44, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(52, 52, 52))
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void task1MouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_task1MouseClicked
        String result = runner.runFirstTask(taskInputField.getText());
        taskOutputField.setText(result);
    }//GEN-LAST:event_task1MouseClicked

    private void task2MouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_task2MouseClicked
        String result = runner.runSecondTask(taskInputField.getText());
        taskOutputField.setText(result);
    }//GEN-LAST:event_task2MouseClicked

    private void task3MouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_task3MouseClicked
        String result = runner.runThirdTask(taskInputField.getText());
        taskOutputField.setText(result);
    }//GEN-LAST:event_task3MouseClicked

    private void task4MouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_task4MouseClicked
        String result = runner.runFourthTask(taskInputField.getText());
        taskOutputField.setText(result);
    }//GEN-LAST:event_task4MouseClicked

    private void task5MouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_task5MouseClicked
        String result = runner.runFifthTask(taskInputField.getText());
        taskOutputField.setText(result);
    }//GEN-LAST:event_task5MouseClicked

    private void fileUploadButtonMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_fileUploadButtonMouseClicked
        try {
            Configuration config = new Configuration();
            config.set("fs.defaultFS", "hdfs://localhost:9000");
            
            FileSystem fileSystem = FileSystem.get(config);
            
            Path pathToHDFSDirectory = new Path(hdfsPathField.getText());
            Path localFilePath = new Path(localFilePathField.getText());
            
            fileSystem.mkdirs(pathToHDFSDirectory);
            
            fileSystem.copyFromLocalFile(localFilePath, pathToHDFSDirectory);
            taskOutputField.setText("File Successfully Copied To Target HDFS Directory");
            
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }//GEN-LAST:event_fileUploadButtonMouseClicked

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        /* Set the Nimbus look and feel */
        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(Menu.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(Menu.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(Menu.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(Menu.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new Menu().setVisible(true);
            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton fileUploadButton;
    private java.awt.TextField hdfsPathField;
    private java.awt.Label label1;
    private java.awt.Label label2;
    private java.awt.Label label3;
    private java.awt.TextField localFilePathField;
    private java.awt.Label runTasksHeader;
    private javax.swing.JButton task1;
    private javax.swing.JButton task2;
    private javax.swing.JButton task3;
    private javax.swing.JButton task4;
    private javax.swing.JButton task5;
    private java.awt.TextField taskInputField;
    private java.awt.Label taskInputLabel;
    private java.awt.TextArea taskOutputField;
    // End of variables declaration//GEN-END:variables
}