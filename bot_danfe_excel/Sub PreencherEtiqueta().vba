Sub PreencherEtiqueta()
    Dim wsEtiqueta As Worksheet
    Dim wsSheet1 As Worksheet
    Dim fornecimento As String
    Dim linha As Long
    Dim qtdadeVolume As Long
    Dim i As Long
    Dim offset As Long
    Dim areaImpressao As String
    Dim rng As Range
    Dim matchValue As Variant
    Dim chaveAcesso As String
    Dim nfCodigo As String
    Dim Pic As Picture

    ' Definindo as planilhas
    Set wsEtiqueta = ThisWorkbook.Sheets("ETIQUETA")
    Set wsSheet1 = ThisWorkbook.Sheets("Sheet1")

    ' Obtendo o fornecimento da célula I2 da planilha ETIQUETA
    fornecimento = CStr(wsEtiqueta.Range("I2").Value)

    ' Definindo o intervalo da nova coluna D (que era B)
    Set rng = wsSheet1.Range("D1:D" & wsSheet1.Cells(wsSheet1.Rows.Count, "D").End(xlUp).Row)

    ' Criando um array com os valores da coluna D como strings
    Dim valoresF() As String
    Dim j As Long
    ReDim valoresF(1 To rng.Rows.Count)

    For j = 1 To rng.Rows.Count
        valoresF(j) = CStr(rng.Cells(j, 1).Value) ' Convertendo todos os valores para string
    Next j

    ' Usando função Match em um array
    On Error Resume Next
    linha = application.WorksheetFunction.Match(fornecimento, valoresF, 0)
    On Error GoTo 0

    ' Verificando se a linha foi encontrada
    If linha = 0 Then
        MsgBox "Fornecimento não encontrado na coluna D."
        Exit Sub
    End If

    ' Preenchendo as células na planilha ETIQUETA
    wsEtiqueta.Range("B1").Value = "1/" & wsSheet1.Cells(linha, "I").Value ' Coluna S agora é I
    wsEtiqueta.Range("D1").Value = "NF " & wsSheet1.Cells(linha, "C").Value ' Coluna E agora é C
    wsEtiqueta.Range("C8:C9").Value = wsSheet1.Cells(linha, "B").Value ' Coluna B
    wsEtiqueta.Range("C10:C11").Value = wsSheet1.Cells(linha, "E").Value & ", " & wsSheet1.Cells(linha, "J").Value ' Coluna N e X agora são F e J
    wsEtiqueta.Range("C12:C13").Value = wsSheet1.Cells(linha, "F").Value ' Coluna O agora é E
    wsEtiqueta.Range("C14:C15").Value = wsSheet1.Cells(linha, "G").Value ' Coluna P agora é G
    wsEtiqueta.Range("C16:C17").Value = wsSheet1.Cells(linha, "H").Value ' Coluna Q agora é H
    wsEtiqueta.Range("C18:C19").Value = wsSheet1.Cells(linha, "K").Value ' Coluna Y agora é K

    ' Obter a quantidade de volumes
    qtdadeVolume = wsSheet1.Cells(linha, "I").Value ' Coluna S agora é I

    ' Definindo a área de impressão total
    areaImpressao = "A1:D" & (qtdadeVolume * 19) ' Calcula o fim da área com base na quantidade de volumes
    wsEtiqueta.PageSetup.PrintArea = areaImpressao

    ' Copiando a chave de acesso da coluna L
    chaveAcesso = wsSheet1.Cells(linha, "L").Value ' Coluna L
    ' Copiando o NF para o código de barras
    nfCodigo = wsSheet1.Cells(linha, "C").Value ' Coluna C

    ' Adicionando a imagem do código de barras da chave de acesso na célula A3
    On Error Resume Next
    wsEtiqueta.Pictures("CódigoDeBarras").Delete ' Remove imagem anterior, se houver
    On Error GoTo 0

    Set Pic = wsEtiqueta.Pictures.Insert("https://barcode.tec-it.com/barcode.ashx?data=" & chaveAcesso & "&code=Code128&dpi=300")
    With Pic
        .Top = wsEtiqueta.Range("B4").Top
        .Left = wsEtiqueta.Range("B4").Left
        .Width = wsEtiqueta.Range("B4").Width
        .Height = wsEtiqueta.Range("B4").Height
        .Name = "CódigoDeBarras" ' Nomeia a imagem para futuras referências
    End With

    ' Adicionando a imagem do código de barras da NF na célula D2
    On Error Resume Next
    wsEtiqueta.Pictures("CodigoNF").Delete ' Remove imagem anterior, se houver
    On Error GoTo 0

    Set Pic = wsEtiqueta.Pictures.Insert("https://barcode.tec-it.com/barcode.ashx?data=" & nfCodigo & "&code=Code128&dpi=300")
    With Pic
        .Top = wsEtiqueta.Range("D2").Top
        .Left = wsEtiqueta.Range("D2").Left - (wsEtiqueta.Range("D2").Width - .Width) / 2 'centraliza
        .Width = wsEtiqueta.Range("D2").Width
        .Height = wsEtiqueta.Range("D2").Height
        .Name = "CodigoNF" ' Nomeia a imagem para futuras referências
    End With

    ' Loop para copiar e colar as etiquetas, mantendo a formatação
    For i = 2 To qtdadeVolume
        offset = (i - 1) * 19 ' Cada etiqueta tem 19 linhas de altura

        ' Copiar e colar a etiqueta
        wsEtiqueta.Rows("1:19").Copy
        wsEtiqueta.Rows(1 + offset).PasteSpecial Paste:=xlPasteAll

        ' Atualizar o valor da célula B correspondente ao volume
        wsEtiqueta.Range("B1").offset(offset, 0).Value = i & "/" & qtdadeVolume

        ' Adiciona uma quebra de página após cada 19 linhas na área de impressão
        If i > 1 Then
            wsEtiqueta.HPageBreaks.Add Before:=wsEtiqueta.Rows(1 + offset)
        End If
    Next i

    ' Imprimindo a planilha ETIQUETA após todas as alterações serem feitas
    'Application.CutCopyMode = False ' Para limpar a seleção do modo de cópia
    'wsEtiqueta.PrintOut
End Sub