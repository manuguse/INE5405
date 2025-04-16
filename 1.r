resumo = function(x) {
    data.frame(
        Media = round(mean(x), 2),
        Mediana = round(median(x), 2),
        Variancia = round(var(x), 2),
        Desvio_Padrao = round(sd(x), 2),
        Coeficiente_de_Variacao = round(sd(x) * 100 / mean(x), 2)
    )
}

ipca = c(5.91, 6.41, 10.67, 6.29, 2.95, 3.75, 4.31, 4.52, 10.06, 5.69, 4.62) 
resumo(ipca)

novo_ipca = ipca
novo_ipca[9] = 30.06
resumo(novo_ipca)

resumo(ipca+2)